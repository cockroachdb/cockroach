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
	"hash"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
			pgerror.Newf(pgcode.InvalidParameterValue, "incorrect number of rounds %d", rounds),
			"supported values: %d, %d",
			defaultRounds, requiredRounds,
		)
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
			pgerror.Newf(pgcode.InvalidParameterValue, "incorrect number of rounds %d", rounds),
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

// genSaltBlowfish generates a salt in the form $2a$rr$ssssssssssssssssssssss
// "2a" - blowfish algorithm
// "rr" - hashing cost (decimal encoded) - 2^rr number of rounds between 04 inclusive and 31 inclusive
// "ssssssssssssssssssssss" - random salt (base64 encoded using itoa64Blowfish)
// see https://github.com/postgres/postgres/blob/586955dddecc95e0003262a3954ae83b68ce0372/contrib/pgcrypto/crypt-gensalt.c#L161
// see bcrypt.newFromPassword (the go implementation generates the salt internally)
func genSaltBlowfish(rounds int) ([]byte, error) {

	if rounds == defaultRounds {
		rounds = 6 // default
	} else if rounds < minRoundsBlowfish || rounds > maxRoundsBlowfish {
		return nil, errors.WithHintf(
			pgerror.Newf(pgcode.InvalidParameterValue, "incorrect number of rounds %d", rounds),
			"supported values: %d or odd number between %d inclusive and %d inclusive",
			defaultRounds, minRoundsBlowfish, maxRoundsBlowfish)
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
