package keyconv

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"

	"github.com/lestrrat-go/blackmagic"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ed25519"
)

// RSAPrivateKey assigns src to dst.
// `dst` should be a pointer to a rsa.PrivateKey.
// `src` may be rsa.PrivateKey, *rsa.PrivateKey, or a jwk.Key
func RSAPrivateKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw rsa.PrivateKey
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce rsa.PrivateKey from %T`, src)
		}
		src = &raw
	}

	var ptr *rsa.PrivateKey
	switch src := src.(type) {
	case rsa.PrivateKey:
		ptr = &src
	case *rsa.PrivateKey:
		ptr = src
	default:
		return errors.Errorf(`expected rsa.PrivateKey or *rsa.PrivateKey, got %T`, src)
	}

	return blackmagic.AssignIfCompatible(dst, ptr)
}

// RSAPublicKey assigns src to dst
// `dst` should be a pointer to a non-zero rsa.PublicKey.
// `src` may be rsa.PublicKey, *rsa.PublicKey, or a jwk.Key
func RSAPublicKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw rsa.PublicKey
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce rsa.PublicKey from %T`, src)
		}
		src = &raw
	}

	var ptr *rsa.PublicKey
	switch src := src.(type) {
	case rsa.PublicKey:
		ptr = &src
	case *rsa.PublicKey:
		ptr = src
	default:
		return errors.Errorf(`expected rsa.PublicKey or *rsa.PublicKey, got %T`, src)
	}

	return blackmagic.AssignIfCompatible(dst, ptr)
}

// ECDSAPrivateKey assigns src to dst, converting its type from a
// non-pointer to a pointer
func ECDSAPrivateKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw ecdsa.PrivateKey
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce ecdsa.PrivateKey from %T`, src)
		}
		src = &raw
	}

	var ptr *ecdsa.PrivateKey
	switch src := src.(type) {
	case ecdsa.PrivateKey:
		ptr = &src
	case *ecdsa.PrivateKey:
		ptr = src
	default:
		return errors.Errorf(`expected ecdsa.PrivateKey or *ecdsa.PrivateKey, got %T`, src)
	}
	return blackmagic.AssignIfCompatible(dst, ptr)
}

// ECDSAPublicKey assigns src to dst, converting its type from a
// non-pointer to a pointer
func ECDSAPublicKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw ecdsa.PublicKey
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce ecdsa.PublicKey from %T`, src)
		}
		src = &raw
	}

	var ptr *ecdsa.PublicKey
	switch src := src.(type) {
	case ecdsa.PublicKey:
		ptr = &src
	case *ecdsa.PublicKey:
		ptr = src
	default:
		return errors.Errorf(`expected ecdsa.PublicKey or *ecdsa.PublicKey, got %T`, src)
	}
	return blackmagic.AssignIfCompatible(dst, ptr)
}

func ByteSliceKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw []byte
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce []byte from %T`, src)
		}
		src = raw
	}

	if _, ok := src.([]byte); !ok {
		return errors.Errorf(`expected []byte, got %T`, src)
	}
	return blackmagic.AssignIfCompatible(dst, src)
}

func Ed25519PrivateKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw ed25519.PrivateKey
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce ed25519.PrivateKey from %T`, src)
		}
		src = &raw
	}

	var ptr *ed25519.PrivateKey
	switch src := src.(type) {
	case ed25519.PrivateKey:
		ptr = &src
	case *ed25519.PrivateKey:
		ptr = src
	default:
		return errors.Errorf(`expected ed25519.PrivateKey or *ed25519.PrivateKey, got %T`, src)
	}
	return blackmagic.AssignIfCompatible(dst, ptr)
}

func Ed25519PublicKey(dst, src interface{}) error {
	if jwkKey, ok := src.(jwk.Key); ok {
		var raw ed25519.PublicKey
		if err := jwkKey.Raw(&raw); err != nil {
			return errors.Wrapf(err, `failed to produce ed25519.PublicKey from %T`, src)
		}
		src = &raw
	}

	var ptr *ed25519.PublicKey
	switch src := src.(type) {
	case ed25519.PublicKey:
		ptr = &src
	case *ed25519.PublicKey:
		ptr = src
	case *crypto.PublicKey:
		tmp, ok := (*src).(ed25519.PublicKey)
		if !ok {
			return errors.New(`failed to retrieve ed25519.PublicKey out of *crypto.PublicKey`)
		}
		ptr = &tmp
	case crypto.PublicKey:
		tmp, ok := src.(ed25519.PublicKey)
		if !ok {
			return errors.New(`failed to retrieve ed25519.PublicKey out of crypto.PublicKey`)
		}
		ptr = &tmp
	default:
		return errors.Errorf(`expected ed25519.PublicKey or *ed25519.PublicKey, got %T`, src)
	}
	return blackmagic.AssignIfCompatible(dst, ptr)
}
