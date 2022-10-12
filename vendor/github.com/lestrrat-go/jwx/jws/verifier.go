package jws

import (
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/pkg/errors"
)

type VerifierFactory interface {
	Create() (Verifier, error)
}
type VerifierFactoryFn func() (Verifier, error)

func (fn VerifierFactoryFn) Create() (Verifier, error) {
	return fn()
}

var verifierDB map[jwa.SignatureAlgorithm]VerifierFactory

// RegisterVerifier is used to register a factory object that creates
// Verifier objects based on the given algorithm.
//
// For example, if you would like to provide a custom verifier for
// jwa.EdDSA, use this function to register a `VerifierFactory`
// (probably in your `init()`)
func RegisterVerifier(alg jwa.SignatureAlgorithm, f VerifierFactory) {
	verifierDB[alg] = f
}

func init() {
	verifierDB = make(map[jwa.SignatureAlgorithm]VerifierFactory)

	for _, alg := range []jwa.SignatureAlgorithm{jwa.RS256, jwa.RS384, jwa.RS512, jwa.PS256, jwa.PS384, jwa.PS512} {
		RegisterVerifier(alg, func(alg jwa.SignatureAlgorithm) VerifierFactory {
			return VerifierFactoryFn(func() (Verifier, error) {
				return newRSAVerifier(alg), nil
			})
		}(alg))
	}

	for _, alg := range []jwa.SignatureAlgorithm{jwa.ES256, jwa.ES384, jwa.ES512, jwa.ES256K} {
		RegisterVerifier(alg, func(alg jwa.SignatureAlgorithm) VerifierFactory {
			return VerifierFactoryFn(func() (Verifier, error) {
				return newECDSAVerifier(alg), nil
			})
		}(alg))
	}

	for _, alg := range []jwa.SignatureAlgorithm{jwa.HS256, jwa.HS384, jwa.HS512} {
		RegisterVerifier(alg, func(alg jwa.SignatureAlgorithm) VerifierFactory {
			return VerifierFactoryFn(func() (Verifier, error) {
				return newHMACVerifier(alg), nil
			})
		}(alg))
	}

	RegisterVerifier(jwa.EdDSA, VerifierFactoryFn(func() (Verifier, error) {
		return newEdDSAVerifier(), nil
	}))
}

// NewVerifier creates a verifier that signs payloads using the given signature algorithm.
func NewVerifier(alg jwa.SignatureAlgorithm) (Verifier, error) {
	f, ok := verifierDB[alg]
	if ok {
		return f.Create()
	}
	return nil, errors.Errorf(`unsupported signature algorithm "%s"`, alg)
}
