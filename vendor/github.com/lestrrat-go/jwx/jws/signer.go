package jws

import (
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/pkg/errors"
)

type SignerFactory interface {
	Create() (Signer, error)
}
type SignerFactoryFn func() (Signer, error)

func (fn SignerFactoryFn) Create() (Signer, error) {
	return fn()
}

var signerDB map[jwa.SignatureAlgorithm]SignerFactory

// RegisterSigner is used to register a factory object that creates
// Signer objects based on the given algorithm.
//
// For example, if you would like to provide a custom signer for
// jwa.EdDSA, use this function to register a `SignerFactory`
// (probably in your `init()`)
func RegisterSigner(alg jwa.SignatureAlgorithm, f SignerFactory) {
	signerDB[alg] = f
}

func init() {
	signerDB = make(map[jwa.SignatureAlgorithm]SignerFactory)

	for _, alg := range []jwa.SignatureAlgorithm{jwa.RS256, jwa.RS384, jwa.RS512, jwa.PS256, jwa.PS384, jwa.PS512} {
		RegisterSigner(alg, func(alg jwa.SignatureAlgorithm) SignerFactory {
			return SignerFactoryFn(func() (Signer, error) {
				return newRSASigner(alg), nil
			})
		}(alg))
	}

	for _, alg := range []jwa.SignatureAlgorithm{jwa.ES256, jwa.ES384, jwa.ES512, jwa.ES256K} {
		RegisterSigner(alg, func(alg jwa.SignatureAlgorithm) SignerFactory {
			return SignerFactoryFn(func() (Signer, error) {
				return newECDSASigner(alg), nil
			})
		}(alg))
	}

	for _, alg := range []jwa.SignatureAlgorithm{jwa.HS256, jwa.HS384, jwa.HS512} {
		RegisterSigner(alg, func(alg jwa.SignatureAlgorithm) SignerFactory {
			return SignerFactoryFn(func() (Signer, error) {
				return newHMACSigner(alg), nil
			})
		}(alg))
	}

	RegisterSigner(jwa.EdDSA, SignerFactoryFn(func() (Signer, error) {
		return newEdDSASigner(), nil
	}))
}

// NewSigner creates a signer that signs payloads using the given signature algorithm.
func NewSigner(alg jwa.SignatureAlgorithm) (Signer, error) {
	f, ok := signerDB[alg]
	if ok {
		return f.Create()
	}
	return nil, errors.Errorf(`unsupported signature algorithm "%s"`, alg)
}
