// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"github.com/xdg-go/scram"
	// Reserved import for PR #74301.
	_ "github.com/xdg-go/stringprep"
	"golang.org/x/crypto/bcrypt"
)

// BcryptCost is the cost to use when hashing passwords.
// It is exposed for testing.
//
// The default value of BcryptCost should increase along with
// computation power.
//
// For estimates, see:
// http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
var BcryptCost = settings.RegisterIntSetting(
	settings.TenantWritable,
	BcryptCostSettingName,
	fmt.Sprintf(
		"the hashing cost to use when storing passwords supplied as cleartext by SQL clients "+
			"with the hashing method crdb-bcrypt (allowed range: %d-%d)",
		bcrypt.MinCost, bcrypt.MaxCost),
	// The default value 10 is equal to bcrypt.DefaultCost.
	// It incurs a password check latency of ~60ms on AMD 3950X 3.7GHz.
	// For reference, value 11 incurs ~110ms latency on the same hw, value 12 incurs ~390ms.
	10,
	func(i int64) error {
		if i < int64(bcrypt.MinCost) || i > int64(bcrypt.MaxCost) {
			return bcrypt.InvalidCostError(int(i))
		}
		return nil
	}).WithPublic()

// BcryptCostSettingName is the name of the cluster setting BcryptCost.
const BcryptCostSettingName = "server.user_login.password_hashes.default_cost.crdb_bcrypt"

const scramMinCost = 4096         // as per RFC 5802.
const scramMaxCost = 240000000000 // arbitrary value to prevent unreasonably long logins.

// ErrEmptyPassword indicates that an empty password was attempted to be set.
var ErrEmptyPassword = errors.New("empty passwords are not permitted")

// ErrPasswordTooShort indicates that a client provided a password
// that was too short according to policy.
var ErrPasswordTooShort = errors.New("password too short")

// ErrUnknownHashMethod is returned by LoadPasswordHash if the hash encoding
// method is not supported.
var ErrUnknownHashMethod = errors.New("unknown hash method")

// HashMethod indicates which password hash method to use.
type HashMethod int8

const (
	// HashInvalidMethod represents invalid hashes.
	// This always fails authentication.
	HashInvalidMethod HashMethod = iota
	// HashMissingPassword represents a virtual hash when there was
	// no password.  This too always fails authentication.
	// We need a different method here than HashInvalidMethod because
	// the authentication code distinguishes the two cases when reporting
	// why authentication fails in audit logs.
	HashMissingPassword
	// HashBCrypt indicates CockroachDB's bespoke bcrypt-based method.
	HashBCrypt
	// HashSCRAMSHA256 indicates SCRAM-SHA-256.
	HashSCRAMSHA256
)

// PasswordHash represents the type of a password hash loaded from a credential store.
type PasswordHash interface {
	fmt.Stringer
	// Method report which hashing method was used.
	Method() HashMethod
	// Size is the size of the in-memory representation of this hash. This
	// is used for memory accounting.
	Size() int
	// compareWithCleartextPassword checks a cleartext password against
	// the hash.
	compareWithCleartextPassword(ctx context.Context, cleartext string) (ok bool, err error)
}

var _ PasswordHash = emptyPassword{}
var _ PasswordHash = invalidHash(nil)
var _ PasswordHash = bcryptHash(nil)
var _ PasswordHash = (*scramHash)(nil)

// emptyPassword represents a virtual hash when there was no password
// to start with.
type emptyPassword struct{}

// String implements fmt.Stringer.
func (e emptyPassword) String() string { return "<missing>" }

// Method is part of the PasswordHash interface.
func (e emptyPassword) Method() HashMethod { return HashMissingPassword }

// Size is part of the PasswordHash interface.
func (e emptyPassword) Size() int { return 0 }

// compareWithCleartextPassword is part of the PasswordHash interface.
func (e emptyPassword) compareWithCleartextPassword(
	ctx context.Context, cleartext string,
) (ok bool, err error) {
	return false, nil
}

// MissingPasswordHash represents the virtual hash when there is no password
// to start with.
var MissingPasswordHash PasswordHash = emptyPassword{}

// invalidHash represents a byte slice that's in an unknown hash format.
// We keep the byte slice around so that it can be passed through
// and re-stored as-is.
type invalidHash []byte

// String implements fmt.Stringer.
func (n invalidHash) String() string { return string(n) }

// Method is part of the PasswordHash interface.
func (n invalidHash) Method() HashMethod { return HashInvalidMethod }

// Size is part of the PasswordHash interface.
func (n invalidHash) Size() int { return len(n) }

// compareWithCleartextPassword is part of the PasswordHash interface.
func (e invalidHash) compareWithCleartextPassword(
	ctx context.Context, cleartext string,
) (ok bool, err error) {
	return false, nil
}

// bcryptHash represents a bcrypt-based hashed password.
// The type is simple since we're offloading the decoding
// of the parameters to the go standard bcrypt package.
type bcryptHash []byte

// String implements fmt.Stringer.
func (b bcryptHash) String() string { return string(b) }

// Method is part of the PasswordHash interface.
func (b bcryptHash) Method() HashMethod { return HashBCrypt }

// Size is part of the PasswordHash interface.
func (b bcryptHash) Size() int { return len(b) }

// scramHash represents a SCRAM-SHA-256 password hash.
type scramHash struct {
	bytes   []byte
	decoded scram.StoredCredentials
}

// String implements fmt.Stringer.
func (s *scramHash) String() string { return string(s.bytes) }

// Method is part of the PasswordHash interface.
func (s *scramHash) Method() HashMethod { return HashSCRAMSHA256 }

// Size is part of the PasswordHash interface.
func (s *scramHash) Size() int {
	return int(unsafe.Sizeof(*s)) + len(s.bytes) + len(s.decoded.Salt) + len(s.decoded.StoredKey) + len(s.decoded.ServerKey)
}

// LoadPasswordHash decodes a password hash loaded as bytes from a credential store.
func LoadPasswordHash(ctx context.Context, storedHash []byte) (res PasswordHash) {
	res = invalidHash(storedHash)
	if len(storedHash) == 0 {
		return emptyPassword{}
	}
	if isBcryptHash(storedHash, false /* strict */) {
		return bcryptHash(storedHash)
	}
	if ok, parts := isSCRAMHash(storedHash); ok {
		return makeSCRAMHash(storedHash, parts, res)
	}
	// Fallthrough: keep the hash, but mark the method as unknown.
	return res
}

var sha256NewSum = sha256.New().Sum(nil)

// TODO(mjibson): properly apply SHA-256 to the password. The current code
// erroneously appends the SHA-256 of the empty hash to the unhashed password
// instead of actually hashing the password. Fixing this requires a somewhat
// complicated backwards compatibility dance. This is not a security issue
// because the round of SHA-256 was only intended to achieve a fixed-length
// input to bcrypt; it is bcrypt that provides the cryptographic security, and
// bcrypt is correctly applied.
func appendEmptySha256(password string) []byte {
	// In the past we incorrectly called the hash.Hash.Sum method. That
	// method uses its argument as a place to put the current hash:
	// it does not add its argument to the current hash. Thus, using
	// h.Sum([]byte(password))) is the equivalent to the below append.
	return append([]byte(password), sha256NewSum...)
}

// CompareHashAndCleartextPassword tests that the provided bytes are equivalent to the
// hash of the supplied password. If the hash is valid but the password does not match,
// no error is returned but the ok boolean is false.
// If an error was detected while using the hash, an error is returned.
func CompareHashAndCleartextPassword(
	ctx context.Context, hashedPassword PasswordHash, password string,
) (ok bool, err error) {
	return hashedPassword.compareWithCleartextPassword(ctx, password)
}

// compareWithCleartextPassword is part of the PasswordHash interface.
func (b bcryptHash) compareWithCleartextPassword(
	ctx context.Context, cleartext string,
) (ok bool, err error) {
	sem := getBcryptSem(ctx)
	alloc, err := sem.Acquire(ctx, 1)
	if err != nil {
		return false, err
	}
	defer alloc.Release()
	err = bcrypt.CompareHashAndPassword([]byte(b), appendEmptySha256(cleartext))
	if err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// compareWithCleartextPassword is part of the PasswordHash interface.
func (s *scramHash) compareWithCleartextPassword(
	ctx context.Context, cleartext string,
) (ok bool, err error) {
	return false, unimplemented.NewWithIssue(42519, "cleartext comparison for SCRAM not supported yet")
}

// HashPassword takes a raw password and returns a bcrypt hashed password.
func HashPassword(ctx context.Context, sv *settings.Values, password string) ([]byte, error) {
	sem := getBcryptSem(ctx)
	alloc, err := sem.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}
	defer alloc.Release()
	return bcrypt.GenerateFromPassword(appendEmptySha256(password), int(BcryptCost.Get(sv)))
}

// AutoDetectPasswordHashes is the cluster setting that configures whether
// the server recognizes pre-hashed passwords.
var AutoDetectPasswordHashes = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"server.user_login.store_client_pre_hashed_passwords.enabled",
	"whether the server accepts to store passwords pre-hashed by clients",
	true,
).WithPublic()

const crdbBcryptPrefix = "CRDB-BCRYPT"

// bcryptHashRe matches the lexical structure of the bcrypt hash
// format supported by CockroachDB. The base64 encoding of the hash
// uses the alphabet used by the bcrypt package:
// "./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
var bcryptHashRe = regexp.MustCompile(`^(` + crdbBcryptPrefix + `)?\$\d[a-z]?\$\d\d\$[0-9A-Za-z\./]{22}[0-9A-Za-z\./]+$`)

// isBcryptHash determines whether hashedPassword is in the CockroachDB bcrypt format.
// If the script parameter is true, then the special "CRDB-BCRYPT" prefix is required.
// This is used e.g. when accepting password hashes in the SQL ALTER USER statement.
// When loading a hash from storage, typically we do not enforce this so as to
// support password hashes stored in earlier versions of CockroachDB.
func isBcryptHash(inputPassword []byte, strict bool) bool {
	if !bcryptHashRe.Match(inputPassword) {
		return false
	}
	if strict && !bytes.HasPrefix(inputPassword, []byte(crdbBcryptPrefix+`$`)) {
		return false
	}
	return true
}

func checkBcryptHash(inputPassword []byte) (ok bool, hashedPassword []byte, err error) {
	if !isBcryptHash(inputPassword, true /* strict */) {
		return false, nil, nil
	}
	// Trim the "CRDB-BCRYPT" prefix. We trim this because previous version
	// CockroachDB nodes do not understand the prefix when stored.
	hashedPassword = inputPassword[len(crdbBcryptPrefix):]
	// The bcrypt.Cost() function parses the hash and checks its syntax.
	_, err = bcrypt.Cost(hashedPassword)
	return true, hashedPassword, err
}

// scramHashRe matches the lexical structure of PostgreSQL's
// pre-computed SCRAM hashes.
//
// This structure is inspired from PosgreSQL's parse_scram_secret() function.
// The base64 encoding uses the alphabet used by pg_b64_encode():
// "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
// The salt must have size >0; the server key pair is two times 32 bytes,
// which always encode to 44 base64 characters.
var scramHashRe = regexp.MustCompile(`^SCRAM-SHA-256\$(\d+):([A-Za-z0-9+/]+=*)\$([A-Za-z0-9+/]{43}=):([A-Za-z0-9+/]{43}=)$`)

func isSCRAMHash(inputPassword []byte) (bool, [][]byte) {
	parts := scramHashRe.FindSubmatch(inputPassword)
	return parts != nil, parts
}

func checkSCRAMHash(inputPassword []byte) (ok bool, hashedPassword []byte, err error) {
	ok, parts := isSCRAMHash(inputPassword)
	if !ok {
		return false, nil, nil
	}
	iters, err := strconv.ParseInt(string(parts[1]), 10, 64)
	if err != nil {
		return true, nil, errors.Wrap(err, "invalid scram-sha-256 iteration count")
	}

	if iters < scramMinCost || iters > scramMaxCost {
		return true, nil, errors.Newf("scram-sha-256 iteration count not in allowed range (%d,%d)", scramMinCost, scramMaxCost)
	}
	return true, inputPassword, nil
}

func makeSCRAMHash(storedHash []byte, parts [][]byte, invalidHash PasswordHash) PasswordHash {
	iters, err := strconv.Atoi(string(parts[1]))
	if err != nil {
		return invalidHash //nolint:returnerrcheck
	}
	salt, err := base64.StdEncoding.DecodeString(string(parts[2]))
	if err != nil {
		return invalidHash //nolint:returnerrcheck
	}
	storedKey, err := base64.StdEncoding.DecodeString(string(parts[3]))
	if err != nil {
		return invalidHash //nolint:returnerrcheck
	}
	serverKey, err := base64.StdEncoding.DecodeString(string(parts[4]))
	if err != nil {
		return invalidHash //nolint:returnerrcheck
	}
	return &scramHash{
		bytes: storedHash,
		decoded: scram.StoredCredentials{
			KeyFactors: scram.KeyFactors{
				Salt:  string(salt),
				Iters: iters,
			},
			StoredKey: storedKey,
			ServerKey: serverKey,
		},
	}
}

func isMD5Hash(hashedPassword []byte) bool {
	// This logic is inspired from PostgreSQL's get_password_type() function.
	return bytes.HasPrefix(hashedPassword, []byte("md5")) &&
		len(hashedPassword) == 35 &&
		len(bytes.Trim(hashedPassword[3:], "0123456789abcdef")) == 0
}

// CheckPasswordHashValidity determines whether a (user-provided)
// password is already hashed, and if already hashed, verifies whether
// the hash is recognized as a valid hash.
// Return values:
// - isPreHashed indicates whether the password is already hashed.
// - supportedScheme indicates whether the scheme is currently supported
//   for authentication. If false, issueNum indicates which github
//   issue to report in the error message.
// - schemeName is the name of the hashing scheme, for inclusion
//   in error messages (no guarantee is made of stability of this string).
// - hashedPassword is a translated version from the input,
//   suitable for storage in the password database.
func CheckPasswordHashValidity(
	ctx context.Context, inputPassword []byte,
) (
	isPreHashed, supportedScheme bool,
	issueNum int,
	schemeName string,
	hashedPassword []byte,
	err error,
) {
	if ok, hashedPassword, err := checkBcryptHash(inputPassword); ok {
		return true, true, 0, "crdb-bcrypt", hashedPassword, err
	}
	if ok, hashedPassword, err := checkSCRAMHash(inputPassword); ok {
		return true, true, 0, "scram-sha-256", hashedPassword, err
	}
	if isMD5Hash(inputPassword) {
		// See: https://github.com/cockroachdb/cockroach/issues/73337
		return true, false /* not supported */, 73337 /* issueNum */, "md5", inputPassword, nil
	}

	return false, false, 0, "", inputPassword, nil
}

// MinPasswordLength is the cluster setting that configures the
// minimum SQL password length.
var MinPasswordLength = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.user_login.min_password_length",
	"the minimum length accepted for passwords set in cleartext via SQL. "+
		"Note that a value lower than 1 is ignored: passwords cannot be empty in any case.",
	1,
	settings.NonNegativeInt,
).WithPublic()

// bcryptSemOnce wraps a semaphore that limits the number of concurrent calls
// to the bcrypt hash functions. This is needed to avoid the risk of a
// DoS attacks by malicious users or broken client apps that would
// starve the server of CPU resources just by computing bcrypt hashes.
//
// We use a sync.Once to delay the creation of the semaphore to the
// first time the password functions are used. This gives a chance to
// the server process to update GOMAXPROCS before we compute the
// maximum amount of concurrency for the semaphore.
var bcryptSemOnce struct {
	sem  *quotapool.IntPool
	once sync.Once
}

// envMaxBcryptConcurrency allows a user to override the semaphore
// configuration using an environment variable.
// If the env var is set to a value >= 1, that value is used.
// Otherwise, a default is computed from the configure GOMAXPROCS.
var envMaxBcryptConcurrency = envutil.EnvOrDefaultInt("COCKROACH_MAX_BCRYPT_CONCURRENCY", 0)

// getBcryptSem retrieves the bcrypt semaphore.
func getBcryptSem(ctx context.Context) *quotapool.IntPool {
	bcryptSemOnce.once.Do(func() {
		var n int
		if envMaxBcryptConcurrency >= 1 {
			// The operator knows better. Use what they tell us to use.
			n = envMaxBcryptConcurrency
		} else {
			// We divide by 8 so that the max CPU usage of bcrypt checks
			// never exceeds ~10% of total CPU resources allocated to this
			// process.
			n = runtime.GOMAXPROCS(-1) / 8
		}
		if n < 1 {
			n = 1
		}
		log.VInfof(ctx, 1, "configured maximum bcrypt concurrency: %d", n)
		bcryptSemOnce.sem = quotapool.NewIntPool("bcrypt", uint64(n))
	})
	return bcryptSemOnce.sem
}
