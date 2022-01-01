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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"github.com/xdg/scram"
	"github.com/xdg/stringprep"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/pbkdf2"
)

// BcryptCost is the cost to use when hashing passwords. It is exposed for
// testing.
//
// BcryptCost should increase along with computation power.
// For estimates, see: http://security.stackexchange.com/questions/17207/recommended-of-rounds-for-bcrypt
// For now, we use the library's default cost.
var BcryptCost = bcrypt.DefaultCost

// SCRAMCost is the cost to use in SCRAM exchanges.
// The value of 4096 is the minimum value recommended by RFC 5802.
// It should be increased along with computation power.
//
// TODO(knz): Do we want to make this configurable via a cluster setting?
var SCRAMCost = 4096

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
	HashInvalidMethod HashMethod = 0
	// HashMissingPassword represents a virtual hash when there was
	// no password.  This too always fails authentication.
	// We need a different method here than HashInvalidMethod because
	// the authentication code distinguishes the two cases when reporting
	// why authentication fails in audit logs.
	HashMissingPassword HashMethod = 1
	// HashBCrypt indicates CockroachDB's bespoke bcrypt-based method.
	// NB: Do not renumber this constant; it is used as value
	// in cluster setting enums.
	HashBCrypt HashMethod = 2
	// HashSCRAMSHA256 indicates SCRAM-SHA-256.
	// NB: Do not renumber this constant; it is used as value
	// in cluster setting enums.
	HashSCRAMSHA256 HashMethod = 3
)

// PasswordHash represents the type of a password hash loaded from a credential store.
type PasswordHash interface {
	fmt.Stringer
	// Method report which hashing method was used.
	Method() HashMethod
	// Size is the size of the in-memory representation of this hash. This
	// is used for memory accounting.
	Size() int
}

// emptyPassword represents a virtual hash when there was no password
// to start with.
type emptyPassword struct{}

// String implements fmt.Stringer.
func (e emptyPassword) String() string { return "<missing>" }

// Method is part of the PasswordHash interface.
func (e emptyPassword) Method() HashMethod { return HashMissingPassword }

// Size is part of the PasswordHash interface.
func (e emptyPassword) Size() int { return 0 }

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

// ScramHash represents a SCRAM-SHA-256 password hash.
type ScramHash struct {
	bytes   []byte
	decoded scram.StoredCredentials
}

// String implements fmt.Stringer.
func (s *ScramHash) String() string { return string(s.bytes) }

// Method is part of the PasswordHash interface.
func (s *ScramHash) Method() HashMethod { return HashSCRAMSHA256 }

// Size is part of the PasswordHash interface.
func (s *ScramHash) Size() int {
	return int(unsafe.Sizeof(*s)) + len(s.bytes) + len(s.decoded.Salt) + len(s.decoded.StoredKey) + len(s.decoded.ServerKey)
}

// StoredCredentials retrieves the SCRAM credential parts.
func (s *ScramHash) StoredCredentials() scram.StoredCredentials {
	return s.decoded
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
	if parts := scramHashRe.FindSubmatch(storedHash); parts != nil {
		iters, err := strconv.Atoi(string(parts[1]))
		if err != nil {
			return res //nolint:returnerrcheck
		}
		salt, err := base64.StdEncoding.DecodeString(string(parts[2]))
		if err != nil {
			return res //nolint:returnerrcheck
		}
		storedKey, err := base64.StdEncoding.DecodeString(string(parts[3]))
		if err != nil {
			return res //nolint:returnerrcheck
		}
		serverKey, err := base64.StdEncoding.DecodeString(string(parts[4]))
		if err != nil {
			return res //nolint:returnerrcheck
		}
		return &ScramHash{
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
	switch ph := hashedPassword.(type) {
	case invalidHash, emptyPassword:
		// Invalid/empty passwords never match.
		return false, nil

	case bcryptHash:
		sem := getExpensiveHashComputeSem(ctx)
		alloc, err := sem.Acquire(ctx, 1)
		if err != nil {
			return false, err
		}
		defer alloc.Release()

		err = bcrypt.CompareHashAndPassword([]byte(ph), appendEmptySha256(password))
		if err != nil {
			if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
				return false, nil
			}
			return false, err
		}
		return true, nil

	case *ScramHash:
		sem := getExpensiveHashComputeSem(ctx)
		alloc, err := sem.Acquire(ctx, 1)
		if err != nil {
			return false, err
		}
		defer alloc.Release()

		// Server-side verification of a plaintext password
		// against a pre-computed stored SCRAM server key.
		//
		// Code inspired from pg's scram_verify_plain_password(),
		// src/backend/libpq/auth-scram.c.
		//
		prepared, err := stringprep.SASLprep.Prepare(password)
		if err != nil {
			// Special PostgreSQL case, quoth comment at the top of
			// auth-scram.c:
			//
			// * - If the password isn't valid UTF-8, or contains characters prohibited
			// *	 by the SASLprep profile, we skip the SASLprep pre-processing and use
			// *	 the raw bytes in calculating the hash.
			prepared = password
		}

		saltedPassword := pbkdf2.Key([]byte(prepared), []byte(ph.decoded.Salt), ph.decoded.Iters, sha256.Size, sha256.New)
		// As per xdg/scram and pg's scram_ServerKey().
		serverKey := computeHMAC(scram.SHA256, saltedPassword, []byte("Server Key"))
		return bytes.Equal(serverKey, ph.decoded.ServerKey), nil

	default:
		return false, errors.AssertionFailedf("programming error: unsupported hash type %T", hashedPassword)
	}
}

// computeHMAC is taken from xdg/scram; sadly it is not exported
// from that package.
func computeHMAC(hg scram.HashGeneratorFcn, key, data []byte) []byte {
	mac := hmac.New(hg, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// PasswordHashMethod is the cluster setting that configures which
// hash method to use when clients request to store a cleartext password.
var PasswordHashMethod = settings.RegisterEnumSetting(
	settings.TenantWritable,
	"server.user_login.password_encryption",
	"which hash method to use to encode cleartext passwords passed via ALTER/CREATE USER/ROLE WITH PASSWORD",
	"crdb-bcrypt",
	map[int64]string{
		int64(HashBCrypt):      "crdb-bcrypt",
		int64(HashSCRAMSHA256): "scram-sha-256",
	},
).WithPublic()

// HashPassword takes a raw password and returns a bcrypt hashed password.
func HashPassword(ctx context.Context, password string, method HashMethod) ([]byte, error) {
	switch method {
	case HashBCrypt:
		sem := getExpensiveHashComputeSem(ctx)
		alloc, err := sem.Acquire(ctx, 1)
		if err != nil {
			return nil, err
		}
		defer alloc.Release()
		return bcrypt.GenerateFromPassword(appendEmptySha256(password), BcryptCost)

	case HashSCRAMSHA256:
		prepared, err := stringprep.SASLprep.Prepare(password)
		if err != nil {
			// Special PostgreSQL case, quoth comment at the top of
			// auth-scram.c:
			//
			// * - If the password isn't valid UTF-8, or contains characters prohibited
			// *	 by the SASLprep profile, we skip the SASLprep pre-processing and use
			// *	 the raw bytes in calculating the hash.
			prepared = password
		}

		// The computation of ServerKey and StoredKey is conveniently provided
		// to us by xdg/scram in the Client method GetStoredCredentials().
		// To use it, we need a client.
		client, err := scram.SHA256.NewClientUnprepped("" /* username: unused */, prepared, "" /* authzID: unused */)
		if err != nil {
			return nil, errors.AssertionFailedf("programming error: client construction should never fail")
		}

		// We also need to generate a random salt ourselves.
		const scramSaltSize = 16 // postgres: SCRAM_DEFAULT_SALT_LEN.
		salt := make([]byte, scramSaltSize)
		if _, err := io.ReadFull(rand.Reader, salt); err != nil {
			return nil, errors.Wrap(err, "generating random salt")
		}

		// The computation of the SCRAM hash is expensive. Use the shared
		// semaphore for it. We reuse the same pattern as the bcrypt case above.
		sem := getExpensiveHashComputeSem(ctx)
		alloc, err := sem.Acquire(ctx, 1)
		if err != nil {
			return nil, err
		}
		defer alloc.Release()
		// Compute the credentials.
		creds := client.GetStoredCredentials(scram.KeyFactors{Iters: SCRAMCost, Salt: string(salt)})
		// Encode them in our standard hash format.
		return encodeScramHash(salt, creds), nil

	default:
		return nil, errors.Newf("unsupported hash method: %v", method)
	}
}

// encodeScramHash encodes the provided SCRAM credentials using the
// standard PostgreSQL / RFC5802 representation.
func encodeScramHash(saltBytes []byte, sc scram.StoredCredentials) []byte {
	b64enc := base64.StdEncoding
	saltLen := b64enc.EncodedLen(len(saltBytes))
	storedKeyLen := b64enc.EncodedLen(len(sc.StoredKey))
	serverKeyLen := b64enc.EncodedLen(len(sc.ServerKey))
	// The representation is:
	//    SCRAM-SHA-256$<iters>:<salt>$<stored key>:<server key>
	// We use a capacity-based slice extension instead of a size-based fill
	// so as to automatically support iteration counts with more than 4 digits.
	res := make([]byte, 0, len(scramPrefix)+1+4 /*iters*/ +1+saltLen+1+storedKeyLen+1+serverKeyLen)
	res = append(res, scramPrefix...)
	res = append(res, '$')
	res = strconv.AppendInt(res, int64(sc.Iters), 10)
	res = append(res, ':')
	res = append(res, make([]byte, saltLen)...)
	b64enc.Encode(res[len(res)-saltLen:], saltBytes)
	res = append(res, '$')
	res = append(res, make([]byte, storedKeyLen)...)
	b64enc.Encode(res[len(res)-storedKeyLen:], sc.StoredKey)
	res = append(res, ':')
	res = append(res, make([]byte, serverKeyLen)...)
	b64enc.Encode(res[len(res)-serverKeyLen:], sc.ServerKey)
	return res
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
func isBcryptHash(hashedPassword []byte, strict bool) bool {
	if bcryptHashRe.Match(hashedPassword) {
		return !strict || bytes.HasPrefix(hashedPassword, []byte(crdbBcryptPrefix+`$`))
	}
	return false
}

const scramPrefix = "SCRAM-SHA-256"

// scramHashRe matches the lexical structure of PostgreSQL's
// pre-computed SCRAM hashes.
//
// This structure is inspired from PosgreSQL's parse_scram_secret() function.
// The base64 encoding uses the alphabet used by pg_b64_encode():
// "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
// The salt must have size >0; the server key pair is two times 32 bytes,
// which always encode to 44 base64 characters.
var scramHashRe = regexp.MustCompile(`^` + scramPrefix + `\$(\d+):([A-Za-z0-9+/]+=*)\$([A-Za-z0-9+/]{43}=):([A-Za-z0-9+/]{43}=)$`)

func isSCRAMHash(hashedPassword []byte) bool {
	return scramHashRe.Match(hashedPassword)
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
//   for authentication. If false, issueLink indicates which github
//   issue to report in the error message.
// - schemeName is the name of the hashing scheme, for inclusion
//   in error messages (no guarantee is made of stability of this string).
// - hashedPassword is a translated version from the input,
//   suitable for storage in the password database.
func CheckPasswordHashValidity(
	ctx context.Context, inputPassword []byte,
) (
	isPreHashed, supportedScheme bool,
	issueLink int,
	schemeName string,
	hashedPassword []byte,
	err error,
) {
	if isBcryptHash(inputPassword, true /* strict */) {
		// Trim the "CRDB-BCRYPT" prefix. We trim this because previous version
		// CockroachDB nodes do not understand the prefix when stored.
		hashedPassword = inputPassword[len(crdbBcryptPrefix):]
		// The bcrypt.Cost() function parses the hash and checks its syntax.
		_, err = bcrypt.Cost(hashedPassword)
		return true, true, 0, "crdb-bcrypt", hashedPassword, err
	}
	if isSCRAMHash(inputPassword) {
		return true, true, 0, "scram-sha-256", inputPassword, nil
	}
	if isMD5Hash(inputPassword) {
		// See: https://github.com/cockroachdb/cockroach/issues/73337
		return true, false /* not supported */, 73337 /* issueLink */, "md5", inputPassword, nil
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
)

// expensiveHashComputeSemOnce wraps a semaphore that limits the
// number of concurrent calls to the bcrypt and sha256 hash
// functions. This is needed to avoid the risk of a DoS attacks by
// malicious users or broken client apps that would starve the server
// of CPU resources just by computing bcrypt hashes.
//
// We use a sync.Once to delay the creation of the semaphore to the
// first time the password functions are used. This gives a chance to
// the server process to update GOMAXPROCS before we compute the
// maximum amount of concurrency for the semaphore.
var expensiveHashComputeSemOnce struct {
	sem  *quotapool.IntPool
	once sync.Once
}

// envMaxHashComputeConcurrency allows a user to override the semaphore
// configuration using an environment variable.
// If the env var is set to a value >= 1, that value is used.
// Otherwise, a default is computed from the configure GOMAXPROCS.
var envMaxHashComputeConcurrency = envutil.EnvOrDefaultInt("COCKROACH_MAX_PW_HASH_COMPUTE_CONCURRENCY", 0)

// getExpensiveHashComputeSem retrieves the bcrypt semaphore.
func getExpensiveHashComputeSem(ctx context.Context) *quotapool.IntPool {
	expensiveHashComputeSemOnce.once.Do(func() {
		var n int
		if envMaxHashComputeConcurrency >= 1 {
			// The operator knows better. Use what they tell us to use.
			n = envMaxHashComputeConcurrency
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
		expensiveHashComputeSemOnce.sem = quotapool.NewIntPool("bcrypt", uint64(n))
	})
	return expensiveHashComputeSemOnce.sem
}
