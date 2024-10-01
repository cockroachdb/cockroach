// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash"
	"strconv"
	"strings"
	_ "unsafe" // required to use go:linkname

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/pgcrypto"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	_ "golang.org/x/crypto/bcrypt" // linked to by go:linkname
)

func init() {
	for k, v := range pgcryptoBuiltins {
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

const cipherSupportedCipherTypeInfo = "The cipher type must have the format `<algorithm>[-<mode>][/pad:<padding>]` where:\n" +
	"* `<algorithm>` is `aes`\n" +
	"* `<mode>` is `cbc` (default)\n" +
	"* `<padding>` is `pkcs` (default) or `none`"

const cipherRequiresEnterpriseLicenseInfo = "This function requires an enterprise license on a CCL distribution."

var pgcryptoBuiltins = map[string]builtinDefinition{

	"crypt": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "password", Typ: types.String}, {Name: "salt", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				password := tree.MustBeDString(args[0])
				salt := tree.MustBeDString(args[1])
				hash, err := crypt(string(password), string(salt))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(hash), nil
			},
			Info:       "Generates a hash based on a password and salt. The hash algorithm and number of rounds if applicable are encoded in the salt.",
			Volatility: volatility.Immutable,
		},
	),

	"decrypt": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "data", Typ: types.Bytes},
				{Name: "key", Typ: types.Bytes},
				{Name: "type", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				data := []byte(tree.MustBeDBytes(args[0]))
				key := []byte(tree.MustBeDBytes(args[1]))
				cipherType := string(tree.MustBeDString(args[2]))
				decryptedData, err := pgcrypto.Decrypt(ctx, evalCtx, data, key, cipherType)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(decryptedData)), nil
			},
			Info: "Decrypt `data` with `key` using the cipher method specified by `type`." +
				"\n\n" + cipherSupportedCipherTypeInfo +
				"\n\n" + cipherRequiresEnterpriseLicenseInfo,
			Volatility: volatility.Immutable,
		},
	),

	"decrypt_iv": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "data", Typ: types.Bytes},
				{Name: "key", Typ: types.Bytes},
				{Name: "iv", Typ: types.Bytes},
				{Name: "type", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				data := []byte(tree.MustBeDBytes(args[0]))
				key := []byte(tree.MustBeDBytes(args[1]))
				iv := []byte(tree.MustBeDBytes(args[2]))
				cipherType := string(tree.MustBeDString(args[3]))
				decryptedData, err := pgcrypto.DecryptIV(ctx, evalCtx, data, key, iv, cipherType)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(decryptedData)), nil
			},
			Info: "Decrypt `data` with `key` using the cipher method specified by `type`. " +
				"If the mode is CBC, the provided `iv` will be used. Otherwise, it will be ignored." +
				"\n\n" + cipherSupportedCipherTypeInfo +
				"\n\n" + cipherRequiresEnterpriseLicenseInfo,
			Volatility: volatility.Immutable,
		},
	),

	"digest": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.String}, {Name: "type", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				alg := tree.MustBeDString(args[1])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hashFunc()
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info: "Computes a binary hash of the given `data`. `type` is the algorithm " +
				"to use (md5, sha1, sha224, sha256, sha384, or sha512).",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.Bytes}, {Name: "type", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				alg := tree.MustBeDString(args[1])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hashFunc()
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info: "Computes a binary hash of the given `data`. `type` is the algorithm " +
				"to use (md5, sha1, sha224, sha256, sha384, or sha512).",
			Volatility: volatility.Immutable,
		},
	),

	"encrypt": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "data", Typ: types.Bytes},
				{Name: "key", Typ: types.Bytes},
				{Name: "type", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				data := []byte(tree.MustBeDBytes(args[0]))
				key := []byte(tree.MustBeDBytes(args[1]))
				cipherType := string(tree.MustBeDString(args[2]))
				encryptedData, err := pgcrypto.Encrypt(ctx, evalCtx, data, key, cipherType)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(encryptedData)), nil
			},
			Info: "Encrypt `data` with `key` using the cipher method specified by `type`." +
				"\n\n" + cipherSupportedCipherTypeInfo +
				"\n\n" + cipherRequiresEnterpriseLicenseInfo,
			Volatility: volatility.Immutable,
		},
	),

	"encrypt_iv": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "data", Typ: types.Bytes},
				{Name: "key", Typ: types.Bytes},
				{Name: "iv", Typ: types.Bytes},
				{Name: "type", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				data := []byte(tree.MustBeDBytes(args[0]))
				key := []byte(tree.MustBeDBytes(args[1]))
				iv := []byte(tree.MustBeDBytes(args[2]))
				cipherType := string(tree.MustBeDString(args[3]))
				encryptedData, err := pgcrypto.EncryptIV(ctx, evalCtx, data, key, iv, cipherType)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(encryptedData)), nil
			},
			Info: "Encrypt `data` with `key` using the cipher method specified by `type`. " +
				"If the mode is CBC, the provided `iv` will be used. Otherwise, it will be ignored." +
				"\n\n" + cipherSupportedCipherTypeInfo +
				"\n\n" + cipherRequiresEnterpriseLicenseInfo,
			Volatility: volatility.Immutable,
		},
	),

	"gen_random_uuid": generateRandomUUID4Impl(),

	"gen_random_bytes": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "count", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				count := int(tree.MustBeDInt(args[0]))
				if count < 1 || count > 1024 {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "length %d is outside the range [1, 1024]", count)
				}
				bytes, err := getRandomBytes(count)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(bytes)), nil
			},
			Info:       "Returns `count` cryptographically strong random bytes. At most 1024 bytes can be extracted at a time.",
			Volatility: volatility.Volatile,
		},
	),

	"gen_salt": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "type", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typ := tree.MustBeDString(args[0])
				salt, err := genSalt(string(typ), 0)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(salt), nil
			},
			Info:       "Generates a salt for input into the `crypt` function using the default number of rounds.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "type", Typ: types.String}, {Name: "iter_count", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				typ := tree.MustBeDString(args[0])
				rounds := tree.MustBeDInt(args[1])
				salt, err := genSalt(string(typ), int(rounds))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(salt), nil
			},
			Info:       "Generates a salt for input into the `crypt` function using `iter_count` number of rounds.",
			Volatility: volatility.Volatile,
		},
	),

	"hmac": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryCrypto},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.String}, {Name: "key", Typ: types.String}, {Name: "type", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				key := tree.MustBeDString(args[1])
				alg := tree.MustBeDString(args[2])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hmac.New(hashFunc, []byte(key))
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info:       "Calculates hashed MAC for `data` with key `key`. `type` is the same as in `digest()`.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.Bytes}, {Name: "key", Typ: types.Bytes}, {Name: "type", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				key := tree.MustBeDBytes(args[1])
				alg := tree.MustBeDString(args[2])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hmac.New(hashFunc, []byte(key))
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info:       "Calculates hashed MAC for `data` with key `key`. `type` is the same as in `digest()`.",
			Volatility: volatility.Immutable,
		},
	),
}

func crypt(password string, salt string) (string, error) {

	var cryptFunc func([]byte, []byte) ([]byte, error)
	if strings.HasPrefix(salt, "$1$") {
		cryptFunc = cryptMD5
	} else if strings.HasPrefix(salt, "$2a$") || strings.HasPrefix(salt, "$2x$") {
		cryptFunc = cryptBlowfish
	} else {
		// TODO(ecwall): implement des/xdes.
		return "", pgerror.New(pgcode.InvalidParameterValue, "invalid salt algorithm")
	}

	output, err := cryptFunc([]byte(password), []byte(salt))
	if err != nil {
		return "", err
	}

	return string(output), nil
}

var md5CryptSwaps = [16]int{12, 6, 0, 13, 7, 1, 14, 8, 2, 15, 9, 3, 5, 10, 4, 11}

func cryptMD5(password, salt []byte) ([]byte, error) {

	if len(salt) > 11 {
		salt = salt[:11] // extra characters are allowed but ignored
	}

	header := salt[:3]
	// can be less than 8 characters (including none) even though gen_salt outputs 8
	randomSalt := salt[3:]

	md5_1 := md5.New()

	md5_1.Write(password)
	md5_1.Write(header)
	md5_1.Write(randomSalt)

	md5_2 := md5.New()
	md5_2.Write(password)
	md5_2.Write(randomSalt)
	md5_2.Write(password)

	for i, mixin := 0, md5_2.Sum(nil); i < len(password); i++ {
		md5_1.Write([]byte{mixin[i%16]})
	}

	for i := len(password); i != 0; i >>= 1 {
		if i%2 == 0 {
			md5_1.Write([]byte{password[0]})
		} else {
			md5_1.Write([]byte{0})
		}
	}

	final := md5_1.Sum(nil)

	for i := 0; i < md5Rounds; i++ {
		md5_3 := md5.New()
		if i%2 == 0 {
			md5_3.Write(final)
		} else {
			md5_3.Write(password)
		}
		if i%3 != 0 {
			md5_3.Write(randomSalt)
		}
		if i%7 != 0 {
			md5_3.Write(password)
		}
		if i%2 == 0 {
			md5_3.Write(password)
		} else {
			md5_3.Write(final)
		}
		final = md5_3.Sum(nil)
	}

	output := make([]byte, len(salt)+1+22)
	copy(output, salt)
	i := len(salt)
	output[i] = '$'
	i++
	v := uint32(0)
	bits := uint32(0)
	for _, swapIndex := range md5CryptSwaps {
		v |= uint32(final[swapIndex]) << bits
		for bits = bits + 8; bits > 6; bits -= 6 {
			output[i] = itoa64[v&0x3f]
			i++
			v >>= 6
		}
	}
	output[i] = itoa64[v&0x3f]

	return output, nil
}

// bcryptLinked accesses private method bcrypt.bcrypt by using go:linkname.
//
//go:linkname bcryptLinked golang.org/x/crypto/bcrypt.bcrypt
func bcryptLinked(password []byte, cost int, salt []byte) ([]byte, error)

const blowfishMinSaltLength = 29

func cryptBlowfish(password, salt []byte) ([]byte, error) {

	saltLength := len(salt)
	if saltLength < blowfishMinSaltLength {
		return nil, errors.WithHintf(
			pgerror.Newf(pgcode.InvalidParameterValue, "invalid salt length %d", saltLength),
			"salt must be at least %d characters long",
			blowfishMinSaltLength,
		)
	}

	salt = salt[:29] // extra characters are allowed but ignored

	rounds, err := strconv.Atoi(string(salt[4:6]))
	if err != nil {
		return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid salt rounds")
	}

	if err := checkBlowfishRounds(rounds); err != nil {
		return nil, err
	}

	if salt[6] != '$' {
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"invalid salt format expected $ but got %c",
			salt[6],
		)
	}

	hashBytes, err := bcryptLinked(password, rounds, salt[7:])
	if err != nil {
		return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid salt encoding")
	}

	output := append(salt, hashBytes...)
	return output, nil
}

// getHashFunc returns a function that will create a new hash.Hash using the
// given algorithm.
func getHashFunc(alg string) (func() hash.Hash, error) {
	switch strings.ToLower(alg) {
	case "md5":
		return md5.New, nil
	case "sha1":
		return sha1.New, nil
	case "sha224":
		return sha256.New224, nil
	case "sha256":
		return sha256.New, nil
	case "sha384":
		return sha512.New384, nil
	case "sha512":
		return sha512.New, nil
	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "cannot use %q, no such hash algorithm", alg)
	}
}

const (
	defaultRounds = 0 // converted to cost that was defaulted to in output if applicable
	md5Rounds     = 1000
	itoa64        = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

var saltFuncMap = map[string]func(int) ([]byte, error){
	"des":  genSaltTraditional,
	"xdes": genSaltExtended,
	"md5":  genSaltMd5,
	"bf":   genSaltBlowfish,
}

// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/px-crypt.c#L132
func genSalt(saltType string, rounds int) (string, error) {

	saltType = strings.ToLower(saltType)

	saltFunc, found := saltFuncMap[saltType]
	if !found {
		keys := make([]string, len(saltFuncMap))
		i := 0
		for k := range saltFuncMap {
			keys[i] = k
			i++
		}
		return "", errors.WithHintf(
			pgerror.Newf(pgcode.InvalidParameterValue, "unknown salt algorithm %q", saltType),
			"supported algorithms: %s",
			keys,
		)
	}

	salt, err := saltFunc(rounds)
	if err != nil {
		return "", err
	}

	return string(salt), nil
}

func getRandomBytes(length int) ([]byte, error) {
	result := make([]byte, length)
	_, err := rand.Read(result)
	return result, err
}

func checkFixedRounds(rounds int, requiredRounds int) error {
	if rounds != defaultRounds && rounds != requiredRounds {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InvalidParameterValue, "invalid number of rounds %d", rounds),
			"supported values: %d, %d",
			defaultRounds, requiredRounds,
		)
	}
	return nil
}

func checkBlowfishRounds(rounds int) error {
	if rounds < minRoundsBlowfish || rounds > maxRoundsBlowfish {
		return errors.WithHintf(
			pgerror.Newf(pgcode.InvalidParameterValue, "invalid number of salt rounds %d", rounds),
			"supported values: %d or between %d inclusive and %d inclusive",
			defaultRounds, minRoundsBlowfish, maxRoundsBlowfish)
	}
	return nil
}

// genSaltTraditional generates a salt in the form ss
// "ss" - random salt (base64 encoded using itoa64)
// des algorithm is implied by having no prefix and does not appear in output
// hashing cost is hard coded to 25 rounds and does not appear in output
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L25
func genSaltTraditional(rounds int) ([]byte, error) {

	// can only be 25
	if err := checkFixedRounds(rounds, 25); err != nil {
		return nil, err
	}

	input, err := getRandomBytes(2)
	if err != nil {
		return nil, err
	}

	return []byte{
		itoa64[input[0]&0x3f],
		itoa64[input[1]&0x3f],
	}, nil
}

const (
	minRoundsExtended = 1
	maxRoundsExtended = 0xffffff
)

// genSaltExtended generates a salt in the form _rrrrssss
// "_" - xdes algorithm
// "rrrr" - hashing cost (base64 encoded using itoa64) - number of rounds odd number between 1 inclusive and 16777215 inclusive
// "ssss" - random salt (base64 encoded using itoa64)
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L43
func genSaltExtended(rounds int) ([]byte, error) {

	if rounds == defaultRounds {
		rounds = 29 * 25 // default
	} else if rounds < minRoundsExtended || rounds > maxRoundsExtended || rounds%2 == 0 {
		// Even iteration counts make it easier to detect weak DES keys from a look at the hash, so they should be avoided.
		return nil, errors.WithHintf(
			pgerror.Newf(pgcode.InvalidParameterValue, "invalid number of rounds %d", rounds),
			"supported values: %d or odd number between %d inclusive and %d inclusive",
			defaultRounds, minRoundsExtended, maxRoundsExtended)
	}

	output := make([]byte, 9)

	output[0] = '_'
	output[1] = itoa64[rounds&0x3f]
	output[2] = itoa64[(rounds>>6)&0x3f]
	output[3] = itoa64[(rounds>>12)&0x3f]
	output[4] = itoa64[(rounds>>18)&0x3f]

	input, err := getRandomBytes(3)
	if err != nil {
		return nil, err
	}

	value := uint32(input[0]) | uint32(input[1])<<8 | uint32(input[2])<<16
	output[5] = itoa64[value&0x3f]
	output[6] = itoa64[(value>>6)&0x3f]
	output[7] = itoa64[(value>>12)&0x3f]
	output[8] = itoa64[(value>>18)&0x3f]

	return output, nil
}

// genSaltMd5 generates a salt in the form $1$ssssssss
// "1" - md5 algorithm
// "ssssssss" - random salt (base64 encoded using itoa64)
// hashing cost is hard coded to 1000 rounds and does not appear in output
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L79
func genSaltMd5(rounds int) ([]byte, error) {

	// can only be 1000
	if err := checkFixedRounds(rounds, md5Rounds); err != nil {
		return nil, err
	}

	input, err := getRandomBytes(6)
	if err != nil {
		return nil, err
	}

	output := make([]byte, 11)

	output[0] = '$'
	output[1] = '1'
	output[2] = '$'

	value := uint32(input[0]) | uint32(input[1])<<8 | uint32(input[2])<<16
	output[3] = itoa64[value&0x3f]
	output[4] = itoa64[(value>>6)&0x3f]
	output[5] = itoa64[(value>>12)&0x3f]
	output[6] = itoa64[(value>>18)&0x3f]

	value = uint32(input[3]) | uint32(input[4])<<8 | uint32(input[5])<<16
	output[7] = itoa64[value&0x3f]
	output[8] = itoa64[(value>>6)&0x3f]
	output[9] = itoa64[(value>>12)&0x3f]
	output[10] = itoa64[(value>>18)&0x3f]

	return output, nil
}

const (
	itoa64Blowfish    = "./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	minRoundsBlowfish = 4
	maxRoundsBlowfish = 31
)

var encodingBlowfish = base64.NewEncoding(itoa64Blowfish).WithPadding(base64.NoPadding)

// genSaltBlowfish generates a salt in the form $2a$rr$ssssssssssssssssssssss
// "2a" - blowfish algorithm
// "rr" - hashing cost (decimal encoded) - 2^rr number of rounds between 04 inclusive and 31 inclusive
// "ssssssssssssssssssssss" - random salt (base64 encoded using itoa64Blowfish)
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L161
// see bcrypt.newFromPassword (the go implementation generates the salt internally)
func genSaltBlowfish(rounds int) ([]byte, error) {

	if rounds == defaultRounds {
		rounds = 6 // default
	} else if err := checkBlowfishRounds(rounds); err != nil {
		return nil, err
	}

	input, err := getRandomBytes(16)
	if err != nil {
		return nil, err
	}

	saltLength := encodingBlowfish.EncodedLen(len(input))

	output := make([]byte, 7+saltLength)
	output[0] = '$'
	output[1] = '2'
	output[2] = 'a'
	output[3] = '$'
	output[4] = '0' + byte(rounds/10)
	output[5] = '0' + byte(rounds%10)
	output[6] = '$'

	encodingBlowfish.Encode(output[7:], input)

	return output, nil
}
