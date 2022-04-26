// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	_ "golang.org/x/crypto/bcrypt"
	"hash"
	"strconv"
	"strings"
	_ "unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func initPgcryptoBuiltins() {
	// Add all pgcryptoBuiltins to the builtins map after a sanity check.
	for k, v := range pgcryptoBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

var pgcryptoBuiltins = map[string]builtinDefinition{
	"crypt": makeBuiltin(
		tree.FunctionProperties{Category: categoryCrypto},
		tree.Overload{
			Types:      tree.ArgTypes{{"password", types.String}, {"salt", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
				password := tree.MustBeDString(args[0])
				salt := tree.MustBeDString(args[1])
				hash, err := crypt(string(password), string(salt))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(hash), nil
			},
			Info:       "Generates hash based on a password and salt. The hash algorithma and number of rounds are encoded in the salt.",
			Volatility: volatility.Volatile,
		},
	),
	"digest": makeBuiltin(
		tree.FunctionProperties{Category: categoryCrypto},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.String}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.LeakProof,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
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

	"gen_random_uuid": generateRandomUUID4Impl,

	"gen_salt": makeBuiltin(
		tree.FunctionProperties{Category: categoryCrypto},
		tree.Overload{
			Types:      tree.ArgTypes{{"type", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Types:      tree.ArgTypes{{"type", types.String}, {"iter_count", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
		tree.FunctionProperties{Category: categoryCrypto},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.String}, {"key", types.String}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.LeakProof,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"key", types.Bytes}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
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

var invalidSalt = pgerror.New(pgcode.InvalidParameterValue, "Invalid salt")

func crypt(password string, salt string) (string, error) {

	var cryptFunc func([]byte, []byte) ([]byte, error)
	if salt[0] == '$' {
		if salt[1] == '1' &&
			salt[2] == '$' {
			cryptFunc = cryptMd5
		} else if salt[1] == '2' &&
			(salt[2] == 'a' || salt[2] == 'x') &&
			salt[3] == '$' {
			cryptFunc = cryptBlowfish
		} else {
			return "", invalidSalt
		}
	} else {
		// todo des/xdes
		return "", invalidSalt
	}

	output, err := cryptFunc([]byte(password), []byte(salt))
	if err != nil {
		return "", err
	}

	return string(output), nil
}

var md5CryptSwaps = [16]int{12, 6, 0, 13, 7, 1, 14, 8, 2, 15, 9, 3, 5, 10, 4, 11}

func cryptMd5(password, fullSalt []byte) ([]byte, error) {

	header := fullSalt[:3]
	salt := fullSalt[3:]

	md5_1 := md5.New()

	md5_1.Write(password)
	md5_1.Write(header)
	md5_1.Write(salt)

	md5_2 := md5.New()
	md5_2.Write(password)
	md5_2.Write(salt)
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

	for i := 0; i < 1000; i++ {
		d2 := md5.New()
		if i&1 == 0 {
			d2.Write(final)
		} else {
			d2.Write(password)
		}

		if i%3 != 0 {
			d2.Write(salt)
		}

		if i%7 != 0 {
			d2.Write(password)
		}

		if i%2 == 0 {
			d2.Write(password)
		} else {
			d2.Write(final)
		}
		final = d2.Sum(nil)
	}

	output := make([]byte, 34)
	copy(output, fullSalt)
	i := len(fullSalt)
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

// bcrypt.bcrypt is private so use this as a workaround to access it
//go:linkname bcryptLinked golang.org/x/crypto/bcrypt.bcrypt
func bcryptLinked(password []byte, cost int, salt []byte) ([]byte, error)

func cryptBlowfish(password, salt []byte) ([]byte, error) {
	if salt[6] != '$' {
		return nil, invalidSalt
	}
	cost, err := strconv.Atoi(string(salt[4:6]))
	if err != nil || cost < 4 || cost > 31 {
		return nil, invalidSalt
	}
	hashBytes, err := bcryptLinked(password, cost, salt[7:])
	if err != nil {
		return nil, err
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
		return "", pgerror.Newf(pgcode.InvalidParameterValue, "Unknown salt algorithm %q. Supported algorithms: %s.", saltType, keys)
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
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Incorrect number of rounds %q. Must be %q or %q.",
			rounds, defaultRounds, requiredRounds)
	}
	return nil
}

// generates a salt in the form ss
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

// generates a salt in the form _rrrrssss
// "_" - xdes algorithm
// "rrrr" - hashing cost (base64 encoded using itoa64) - number of rounds odd number between 1 inclusive and 16777215 inclusive
// "ssss" - random salt (base64 encoded using itoa64)
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L43
func genSaltExtended(rounds int) ([]byte, error) {

	if rounds == defaultRounds {
		rounds = 29 * 25 // default
	} else if rounds < minRoundsExtended || rounds > maxRoundsExtended || rounds%2 == 0 {
		// Even iteration counts make it easier to detect weak DES keys from a look at the hash, so they should be avoided.
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Incorrect number of rounds %q. Must be %q or odd number between %q inclusive and %q inclusive.",
			rounds, defaultRounds, minRoundsExtended, maxRoundsExtended)
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

// generates a salt in the form $1$ssssssss
// "1" - md5 algorithm
// "ssssssss" - random salt (base64 encoded using itoa64)
// hashing cost is hard coded to 1000 rounds and does not appear in output
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L79
func genSaltMd5(rounds int) ([]byte, error) {

	// can only be 1000
	if err := checkFixedRounds(rounds, 1000); err != nil {
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

// generates a salt in the form $2a$rr$ssssssssssssssssssssss
// "2a" - blowfish algorithm
// "rr" - hashing cost (decimal encoded) - 2^rr number of rounds between 04 inclusive and 31 inclusive
// "ssssssssssssssssssssss" - random salt (base64 encoded using itoa64Blowfish)
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L161
// see bcrypt.base64Encode
func genSaltBlowfish(rounds int) ([]byte, error) {

	if rounds == defaultRounds {
		rounds = 6 // default
	} else if rounds < minRoundsBlowfish || rounds > maxRoundsBlowfish {
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"Incorrect number of rounds %q. Must be %q or between %q inclusive and %q inclusive.",
			rounds, defaultRounds, minRoundsBlowfish, maxRoundsBlowfish)
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
