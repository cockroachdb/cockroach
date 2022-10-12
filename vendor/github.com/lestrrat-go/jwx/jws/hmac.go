package jws

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/lestrrat-go/jwx/internal/keyconv"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/pkg/errors"
)

var hmacSignFuncs = map[jwa.SignatureAlgorithm]hmacSignFunc{}

func init() {
	algs := map[jwa.SignatureAlgorithm]func() hash.Hash{
		jwa.HS256: sha256.New,
		jwa.HS384: sha512.New384,
		jwa.HS512: sha512.New,
	}

	for alg, h := range algs {
		hmacSignFuncs[alg] = makeHMACSignFunc(h)
	}
}

func newHMACSigner(alg jwa.SignatureAlgorithm) Signer {
	return &HMACSigner{
		alg:  alg,
		sign: hmacSignFuncs[alg], // we know this will succeed
	}
}

func makeHMACSignFunc(hfunc func() hash.Hash) hmacSignFunc {
	return func(payload []byte, key []byte) ([]byte, error) {
		h := hmac.New(hfunc, key)
		if _, err := h.Write(payload); err != nil {
			return nil, errors.Wrap(err, "failed to write payload using hmac")
		}
		return h.Sum(nil), nil
	}
}

func (s HMACSigner) Algorithm() jwa.SignatureAlgorithm {
	return s.alg
}

func (s HMACSigner) Sign(payload []byte, key interface{}) ([]byte, error) {
	var hmackey []byte
	if err := keyconv.ByteSliceKey(&hmackey, key); err != nil {
		return nil, errors.Wrapf(err, `invalid key type %T. []byte is required`, key)
	}

	if len(hmackey) == 0 {
		return nil, errors.New(`missing key while signing payload`)
	}

	return s.sign(payload, hmackey)
}

func newHMACVerifier(alg jwa.SignatureAlgorithm) Verifier {
	s := newHMACSigner(alg)
	return &HMACVerifier{signer: s}
}

func (v HMACVerifier) Verify(payload, signature []byte, key interface{}) (err error) {
	expected, err := v.signer.Sign(payload, key)
	if err != nil {
		return errors.Wrap(err, `failed to generated signature`)
	}

	if !hmac.Equal(signature, expected) {
		return errors.New(`failed to match hmac signature`)
	}
	return nil
}
