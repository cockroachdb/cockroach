// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package password

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/xdg-go/pbkdf2"
	"github.com/xdg-go/scram"
	"github.com/xdg-go/stringprep"
	"golang.org/x/crypto/bcrypt"
)

const (
	// DefaultBcryptCost is the hashing cost for the hashing method crdb-bcrypt.
	DefaultBcryptCost = 10
	// DefaultSCRAMCost is the hashing cost for the hashing method SCRAM.
	DefaultSCRAMCost = 10610
	// ScramMinCost is as per RFC 5802.
	ScramMinCost = 4096
	// ScramMaxCost is an arbitrary value to prevent unreasonably long logins
	ScramMaxCost = 240000000000
)

const (
	crdbBcryptPrefix = "CRDB-BCRYPT"
	scramPrefix      = "SCRAM-SHA-256"
)

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

func (h HashMethod) String() string {
	switch h {
	case HashInvalidMethod:
		return "<invalid>"
	case HashMissingPassword:
		return "<missing password>"
	case HashBCrypt:
		return "crdb-bcrypt"
	case HashSCRAMSHA256:
		return "scram-sha-256"
	default:
		panic(errors.AssertionFailedf("programming error: unknown hash method %d", int(h)))
	}
}

// GetDefaultCost retrieves the default hashing cost for the given method.
func (h HashMethod) GetDefaultCost() int {
	switch h {
	case HashBCrypt:
		return DefaultBcryptCost
	case HashSCRAMSHA256:
		return DefaultSCRAMCost
	default:
		return -1
	}
}

// LookupMethod returns the HashMethod by name.
func LookupMethod(s string) HashMethod {
	switch s {
	case HashBCrypt.String():
		return HashBCrypt
	case HashSCRAMSHA256.String():
		return HashSCRAMSHA256
	default:
		return HashInvalidMethod
	}
}

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
	compareWithCleartextPassword(ctx context.Context, cleartext string, hashSem HashSemaphore) (ok bool, err error)
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
	ctx context.Context, cleartext string, hashSem HashSemaphore,
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
func (n invalidHash) compareWithCleartextPassword(
	ctx context.Context, cleartext string, hashSem HashSemaphore,
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

// GetSCRAMStoredCredentials retrieves the SCRAM credential parts.
// The caller is responsible for ensuring the hash has method SCRAM-SHA-256.
func GetSCRAMStoredCredentials(hash PasswordHash) (ok bool, creds scram.StoredCredentials) {
	h, ok := hash.(*scramHash)
	if ok {
		return ok, h.decoded
	}
	return false, creds
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

// AppendEmptySha256 is to append the SHA-256 of the empty hash to the unhashed
// password.
// TODO(mjibson): properly apply SHA-256 to the password. The current code
// erroneously appends the SHA-256 of the empty hash to the unhashed password
// instead of actually hashing the password. Fixing this requires a somewhat
// complicated backwards compatibility dance. This is not a security issue
// because the round of SHA-256 was only intended to achieve a fixed-length
// input to bcrypt; it is bcrypt that provides the cryptographic security, and
// bcrypt is correctly applied.
func AppendEmptySha256(password string) []byte {
	// In the past we incorrectly called the hash.Hash.Sum method. That
	// method uses its argument as a place to put the current hash:
	// it does not add its argument to the current hash. Thus, using
	// h.Sum([]byte(password))) is the equivalent to the below append.
	return append([]byte(password), sha256NewSum...)
}

// HashSemaphore is the type of a semaphore that can be provided
// to hashing functions to control the rate of password hashes.
type HashSemaphore func(context.Context) (func(), error)

// CompareHashAndCleartextPassword tests that the provided bytes are equivalent to the
// hash of the supplied password. If the hash is valid but the password does not match,
// no error is returned but the ok boolean is false.
// If an error was detected while using the hash, an error is returned.
// If sem is non null, it is acquired before computing a hash.
func CompareHashAndCleartextPassword(
	ctx context.Context, hashedPassword PasswordHash, password string, hashSem HashSemaphore,
) (ok bool, err error) {
	return hashedPassword.compareWithCleartextPassword(ctx, password, hashSem)
}

// compareWithCleartextPassword is part of the PasswordHash interface.
func (b bcryptHash) compareWithCleartextPassword(
	ctx context.Context, cleartext string, hashSem HashSemaphore,
) (ok bool, err error) {
	if hashSem != nil {
		alloc, err := hashSem(ctx)
		if err != nil {
			return false, err
		}
		defer alloc()
	}

	err = bcrypt.CompareHashAndPassword([]byte(b), AppendEmptySha256(cleartext))
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
	ctx context.Context, cleartext string, hashSem HashSemaphore,
) (ok bool, err error) {
	if hashSem != nil {
		alloc, err := hashSem(ctx)
		if err != nil {
			return false, err
		}
		defer alloc()
	}

	// Server-side verification of a plaintext password
	// against a pre-computed stored SCRAM server key.
	//
	// Code inspired from pg's scram_verify_plain_password(),
	// src/backend/libpq/auth-scram.c.
	//
	prepared, err := stringprep.SASLprep.Prepare(cleartext)
	if err != nil {
		// Special PostgreSQL case, quoth comment at the top of
		// auth-scram.c:
		//
		// * - If the password isn't valid UTF-8, or contains characters prohibited
		// *	 by the SASLprep profile, we skip the SASLprep pre-processing and use
		// *	 the raw bytes in calculating the hash.
		prepared = cleartext
	}

	saltedPassword := pbkdf2.Key([]byte(prepared), []byte(s.decoded.Salt), s.decoded.Iters, sha256.Size, sha256.New)
	// As per xdg-go/scram and pg's scram_ServerKey().
	// Note: the string "Server Key" is part of the SCRAM algorithm,
	// see IETF RFC 5802.
	serverKey := computeHMAC(scram.SHA256, saltedPassword, []byte("Server Key"))
	return bytes.Equal(serverKey, s.decoded.ServerKey), nil
}

// computeHMAC is taken from xdg-go/scram; sadly it is not exported
// from that package.
func computeHMAC(hg scram.HashGeneratorFcn, key, data []byte) []byte {
	mac := hmac.New(hg, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// HashPassword takes a raw password and returns a hashed password, hashed
// using the currently configured method.
func HashPassword(
	ctx context.Context, cost int, hashMethod HashMethod, password string, hashSem HashSemaphore,
) ([]byte, error) {
	switch hashMethod {
	case HashBCrypt:
		if hashSem != nil {
			alloc, err := hashSem(ctx)
			if err != nil {
				return nil, err
			}
			defer alloc()
		}
		ret, err := bcrypt.GenerateFromPassword(AppendEmptySha256(password), cost)
		if errors.Is(err, bcrypt.ErrPasswordTooLong) {
			err = pgerror.Wrap(err, pgcode.InvalidPassword, "")
		}
		return ret, err

	case HashSCRAMSHA256:
		return hashPasswordUsingSCRAM(ctx, cost, password, hashSem)

	default:
		return nil, errors.Newf("unsupported hash method: %v", hashMethod)
	}
}

func hashPasswordUsingSCRAM(
	ctx context.Context, cost int, cleartext string, hashSem HashSemaphore,
) ([]byte, error) {
	prepared, err := stringprep.SASLprep.Prepare(cleartext)
	if err != nil {
		// Special PostgreSQL case, quoth comment at the top of
		// auth-scram.c:
		//
		// * - If the password isn't valid UTF-8, or contains characters prohibited
		// *	 by the SASLprep profile, we skip the SASLprep pre-processing and use
		// *	 the raw bytes in calculating the hash.
		prepared = cleartext
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
	if hashSem != nil {
		alloc, err := hashSem(ctx)
		if err != nil {
			return nil, err
		}
		defer alloc()
	}
	// Compute the credentials.
	creds := client.GetStoredCredentials(scram.KeyFactors{Iters: cost, Salt: string(salt)})
	// Encode them in our standard hash format.
	return encodeScramHash(salt, creds), nil
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
var scramHashRe = regexp.MustCompile(`^` + scramPrefix + `\$(\d+):([A-Za-z0-9+/]+=*)\$([A-Za-z0-9+/]{43}=):([A-Za-z0-9+/]{43}=)$`)

// scramParts is an intermediate type to connect the output of
// isSCRAMHash() to makeSCRAMHash(), so that the latter cannot be
// legitimately used without the former.
type scramParts [][]byte

func (sp scramParts) getIters() (int, error) {
	return strconv.Atoi(string(sp[1]))
}

func (sp scramParts) getSalt() ([]byte, error) {
	return base64.StdEncoding.DecodeString(string(sp[2]))
}

func (sp scramParts) getStoredKey() ([]byte, error) {
	return base64.StdEncoding.DecodeString(string(sp[3]))
}

func (sp scramParts) getServerKey() ([]byte, error) {
	return base64.StdEncoding.DecodeString(string(sp[4]))
}

func isSCRAMHash(inputPassword []byte) (bool, scramParts) {
	parts := scramHashRe.FindSubmatch(inputPassword)
	if parts == nil {
		return false, nil
	}
	if len(parts) != 5 {
		panic(errors.AssertionFailedf("programming error: scramParts type must have same length as regexp groups"))
	}
	return true, scramParts(parts)
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

	if iters < ScramMinCost || iters > ScramMaxCost {
		return true, nil, errors.Newf("scram-sha-256 iteration count not in allowed range (%d,%d)", ScramMinCost, ScramMaxCost)
	}
	return true, inputPassword, nil
}

// makeSCRAMHash constructs a PasswordHash using the output of a
// previous call to isSCRAMHash().
func makeSCRAMHash(storedHash []byte, parts scramParts, invalidHash PasswordHash) PasswordHash {
	iters, err := parts.getIters()
	if err != nil || iters < ScramMinCost {
		return invalidHash //nolint:returnerrcheck
	}
	salt, err := parts.getSalt()
	if err != nil {
		return invalidHash //nolint:returnerrcheck
	}
	storedKey, err := parts.getStoredKey()
	if err != nil {
		return invalidHash //nolint:returnerrcheck
	}
	serverKey, err := parts.getServerKey()
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
//   - isPreHashed indicates whether the password is already hashed.
//   - supportedScheme indicates whether the scheme is currently supported
//     for authentication. If false, issueNum indicates which github
//     issue to report in the error message.
//   - schemeName is the name of the hashing scheme, for inclusion
//     in error messages (no guarantee is made of stability of this string).
//   - hashedPassword is a translated version from the input,
//     suitable for storage in the password database.
func CheckPasswordHashValidity(
	inputPassword []byte,
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

// BcryptCostToSCRAMIterCount maps the bcrypt cost in a pre-hashed
// password using the crdb-bcrypt method to an “equivalent” cost
// (iteration count) for the scram-sha-256 method. This is used to
// automatically upgrade clusters from crdb-bcrypt to scram-sha-256.
//
// This mapping was computed so that given a starting bcrypt cost, the
// latency of authentication using SCRAM-SHA-256 with an iter count
// computed by this mapping would be comparable to that when using bcrypt.
//
// For example, with bcrypt cost 10, if the authn latency is ~60ms
// (an actual measurement on current hardware as of this writing),
// the mapping gives SCRAM authn latency of ~60ms too.
//
// The actual values were computed as follows:
//  1. measure the bcrypt authentication cost for costs 1-19.
//  2. assuming the bcrypt latency is a_bcrypt*2^c + b_bcrypt, where c
//     is the bcrypt cost, use statistical regression to derive
//     a_bcrypt and b_bcrypt. (we found b_bcrypt to be negligible.)
//  3. measure the SCRAM authn cost for iter counts 4096-1000000,
//     *on the same hardware*.
//  4. assuming the SCRAM latency is a_scram*c + b_scram,
//     where c is the SCRAM iter count, use stat regression
//     to derive a_scram and b_scram. (we found b_scram to be negligible).
//  5. for each bcrypt cost, compute scram iter count = a_bcrypt * 2^cost_bcrypt / a_scram.
//
// The speed of the CPU used for the measurements is equally
// represented in a_bcrypt and a_scram, so the formula eliminates any
// CPU-specific factor.
//
// An alternative approach would have been to choose a SCRAM-SHA-256
// mapping that gives equivalent difficulty at bruteforcing passwords
// given access to the hashes and access to ASICs/GPUs. If that was
// the goal, we would derive higher iteration counts by a factor of at
// least 10x.
// (From bdarnell's analysis: In 1 second, a CPU can do 1M iterations
// of hmac-sha256 or 16k iterations of bcrypt, a ratio of 60:1. A GPU
// can do 2G iterations of hmac-sha256 or 3M iterations of 600:1.)
//
// However, this would also increase the user login
// latency by a factor of 10x, which we consider unacceptable from a
// usability perspective for a *default* mapping.
// Instead, for CockroachDB we will recommend in docs that
// users define passwords with a high complexity, so that
// the entropy of the password itself compounds with the complexity
// of the SCRAM hash.
// As of this writing this is already the approach taken in
// CockroachCloud, where passwords are auto-generated.
//
// Meanwhile, we are also announcing this trade-off in release notes:
// an operator who lets their end-users select their own passwords,
// and wishes to prioritize bruteforcing hardness at the
// expense of login latency, will be free to adjust the setting
// server.user_login.password_hashes.default_cost.scram_sha_256 and
// re-encode their passwords.
var BcryptCostToSCRAMIterCount = []int64{
	0,           // 0-3 are not valid bcrypt costs.
	0,           // 0-3 are not valid bcrypt costs.
	0,           // 0-3 are not valid bcrypt costs.
	0,           // 0-3 are not valid bcrypt costs.
	4096,        // 4 - special case to select lowest cost possible. Model would predict 1288.
	4096,        // 5 - special case to select lowest cost possible. Model would predict 1654.
	4096,        // 6 - special case to select lowest cost possible. Model would predict 2384.
	4096,        // 7 - special case to select lowest cost possible. Model would predict 3846.
	6768,        // 8
	8768,        // 9
	10610,       // 10 - common default, 50-100ms login latency on 2021 hardware
	24302,       // 11
	47682,       // 12
	94441,       // 13
	187958,      // 14
	374993,      // 15
	749063,      // 16
	1497202,     // 17
	2993481,     // 18
	5986039,     // 19
	11971154,    // 20
	23941385,    // 21
	47881848,    // 22
	95762772,    // 23
	191524622,   // 24
	383048320,   // 25
	766095717,   // 26
	1532190512,  // 27
	3064380100,  // 28
	6128759277,  // 29
	12257517630, // 30
	24515034337, // 31
}

// ScramIterCountToBcryptCost computes the inverse of the
// BcryptCostToSCRAMIterCount mapping.
func ScramIterCountToBcryptCost(scramIters int) int {
	for i, thisIterCount := range BcryptCostToSCRAMIterCount {
		if i >= bcrypt.MaxCost {
			return bcrypt.MaxCost
		}
		if int64(scramIters) >= thisIterCount && int64(scramIters) < BcryptCostToSCRAMIterCount[i+1] {
			return i
		}
	}
	return 0
}

// MaybeConvertPasswordHash looks at the cleartext and the hashed
// password and determines whether the hash can be converted from/to
// crdb-bcrypt to/from scram-sha-256. If it can, it computes an equivalent
// hash and returns it.
//
// See the documentation on BcryptCostToSCRAMIterCount[] for details.
func MaybeConvertPasswordHash(
	ctx context.Context,
	autoUpgradePasswordHashes, autoDowngradePasswordHashesBool, autoRehashOnCostChangeBool bool,
	configuredHashMethod HashMethod,
	configuredSCRAMCost int64,
	cleartext string,
	hashed PasswordHash,
	hashSem HashSemaphore,
	logEvent func(context.Context, string, ...interface{}),
) (converted bool, prevHashBytes, newHashBytes []byte, newMethod string, err error) {
	bh, isBcrypt := hashed.(bcryptHash)
	sh, isScram := hashed.(*scramHash)

	// First check upgrading hashes. We need the following:
	// - password currently hashed using crdb-bcrypt.
	// - conversion enabled by cluster setting.
	// - the configured default method is scram-sha-256.
	if isBcrypt && autoUpgradePasswordHashes && configuredHashMethod == HashSCRAMSHA256 {
		bcryptCost, err := bcrypt.Cost(bh)
		if err != nil {
			// The caller should only call this function after authentication
			// has succeeded, so the bcrypt cost should have been validated
			// already.
			return false, nil, nil, "", errors.NewAssertionErrorWithWrappedErrf(err, "programming error: authn succeeded but invalid bcrypt hash")
		}
		if bcryptCost < bcrypt.MinCost || bcryptCost > bcrypt.MaxCost || BcryptCostToSCRAMIterCount[bcryptCost] == 0 {
			// The bcryptCost was smaller than 4 or greater than 31? That's a violation of a bcrypt invariant.
			// Or perhaps the BcryptCostToSCRAMIterCount was incorrectly modified and there's a hole with value zero.
			return false, nil, nil, "", errors.AssertionFailedf("unexpected: bcrypt cost %d is out of bounds or has no mapping", bcryptCost)
		}

		scramIterCount := BcryptCostToSCRAMIterCount[bcryptCost]
		if scramIterCount > math.MaxInt {
			// scramIterCount is an int64. However, the SCRAM library we're using (xdg/scram) uses
			// an int for the iteration count. This is not an issue when this code is running
			// on a 64-bit platform, where sizeof(int) == sizeof(int64). However, when running
			// on 32-bit, we can't allow a conversion to proceed because it would potentially
			// truncate the iter count to a low value.
			// However, this situation is not an error. The hash could still be converted later
			// when the system is upgraded to 64-bit.
			return false, nil, nil, "", nil
		}

		if bcryptCost > 10 {
			// Tell the logs that we're doing a conversion. This is important
			// if the new cost is high and the operation takes a long time, so
			// the operator knows what's up.
			//
			// Note: this is an informational message for troubleshooting
			// purposes, and therefore is best sent to the DEV channel. The
			// structured event that reports that the conversion has completed
			// (and the new credentials were stored) is sent by the SQL code
			// that also owns the storing of the new credentials.
			logEvent(ctx, "hash conversion: computing a SCRAM hash with iteration count %d (from bcrypt cost %d)", scramIterCount, bcryptCost)
		}

		newHash, err := hashAndValidate(ctx, int(scramIterCount), HashSCRAMSHA256, cleartext, hashSem)
		if err != nil {
			// This call only fail with hard errors.
			return false, nil, nil, "", err
		}
		return true, bh, newHash, HashSCRAMSHA256.String(), nil
	}

	// Now check updating SCRAM hashes with a new cost. We need the following:
	// - password currently hashed using scram-sha-256.
	// - cost conversion enabled by cluster setting.
	// - the configured default method is scram-sha-256.
	// - the current password cost is different than the configured default cost.
	if isScram && autoRehashOnCostChangeBool && configuredHashMethod == HashSCRAMSHA256 {
		ok, parts := isSCRAMHash(sh.bytes)
		if !ok {
			// The caller should only call this function after authentication
			// has succeeded, so the scram hash should have been validated
			// already.
			return false, nil, nil, "", errors.AssertionFailedf("programming error: authn succeeded but invalid scram hash")
		}
		scramIters, err := parts.getIters()
		if err != nil {
			// The caller should only call this function after authentication
			// has succeeded, so the scram iters should have been validated
			// already.
			return false, nil, nil, "", errors.NewAssertionErrorWithWrappedErrf(err, "programming error: authn succeeded but invalid scram hash")
		}

		if scramIters < ScramMinCost || scramIters > ScramMaxCost {
			// The scramIters being out of range is a violation of our SCRAM preconditions.
			return false, nil, nil, "", errors.AssertionFailedf("unexpected: scram iteration count %d is out of bounds", scramIters)
		}

		if configuredSCRAMCost != int64(scramIters) {
			newHash, err := hashAndValidate(ctx, int(configuredSCRAMCost), HashSCRAMSHA256, cleartext, hashSem)
			if err != nil {
				// This call only fail with hard errors.
				return false, nil, nil, "", err
			}
			return true, sh.bytes, newHash, HashSCRAMSHA256.String(), nil
		}
	}

	// Now check downgrading hashes. We need the following:
	// - password currently hashed using scram-sha-256.
	// - conversion enabled by cluster setting.
	// - the configured default method is crdb-bcrypt.
	if isScram && autoDowngradePasswordHashesBool && configuredHashMethod == HashBCrypt {
		ok, parts := isSCRAMHash(sh.bytes)
		if !ok {
			// The caller should only call this function after authentication
			// has succeeded, so the scram hash should have been validated
			// already.
			return false, nil, nil, "", errors.AssertionFailedf("programming error: authn succeeded but invalid scram hash")
		}
		scramIters, err := parts.getIters()
		if err != nil {
			// The caller should only call this function after authentication
			// has succeeded, so the scram iters should have been validated
			// already.
			return false, nil, nil, "", errors.NewAssertionErrorWithWrappedErrf(err, "programming error: authn succeeded but invalid scram hash")
		}
		if scramIters < ScramMinCost || scramIters > ScramMaxCost {
			// The scramIters being out of range is a violation of our SCRAM preconditions.
			return false, nil, nil, "", errors.AssertionFailedf("unexpected: scram iteration count %d is out of bounds", scramIters)
		}

		bcryptCost := ScramIterCountToBcryptCost(scramIters)
		if bcryptCost > 10 {
			// Tell the logs that we're doing a conversion. This is important
			// if the new cost is high and the operation takes a long time, so
			// the operator knows what's up.
			//
			// Note: this is an informational message for troubleshooting
			// purposes, and therefore is best sent to the DEV channel. The
			// structured event that reports that the conversion has completed
			// (and the new credentials were stored) is sent by the SQL code
			// that also owns the storing of the new credentials.
			logEvent(ctx, "hash conversion: computing a bcrypt hash with cost %d (from SCRAM iteration count %d)", bcryptCost, scramIters)
		}

		newHash, err := hashAndValidate(ctx, bcryptCost, HashBCrypt, cleartext, hashSem)
		if err != nil {
			// This call only fail with hard errors.
			return false, nil, nil, "", err
		}
		return true, sh.bytes, newHash, HashBCrypt.String(), nil
	}

	// Nothing to do.
	return false, nil, nil, "", nil
}

// hashAndValidate hashes the cleartext password and validates that it can
// be decoded.
func hashAndValidate(
	ctx context.Context, cost int, newHashMethod HashMethod, cleartext string, hashSem HashSemaphore,
) (newHashBytes []byte, err error) {
	rawHash, err := HashPassword(ctx, cost, newHashMethod, cleartext, hashSem)
	if err != nil {
		// This call only fail with hard errors.
		return nil, err
	}

	// Check the raw hash can be decoded using our decoder.
	// This checks that we're able to re-parse what we just generated,
	// which is a safety mechanism to ensure the result is valid.
	newHash := LoadPasswordHash(ctx, rawHash)
	if newHash.Method() != newHashMethod {
		// In contrast to logs, we don't want the details of the hash in the error with %+v.
		return nil, errors.AssertionFailedf("programming error: re-hash failed to produce %s hash, produced %T instead", newHashMethod, newHash)
	}
	return rawHash, nil
}
