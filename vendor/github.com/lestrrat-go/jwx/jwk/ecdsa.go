package jwk

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"math/big"

	"github.com/lestrrat-go/blackmagic"
	"github.com/lestrrat-go/jwx/internal/base64"
	"github.com/lestrrat-go/jwx/internal/ecutil"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/pkg/errors"
)

func init() {
	ecutil.RegisterCurve(elliptic.P256(), jwa.P256)
	ecutil.RegisterCurve(elliptic.P384(), jwa.P384)
	ecutil.RegisterCurve(elliptic.P521(), jwa.P521)
}

func (k *ecdsaPublicKey) FromRaw(rawKey *ecdsa.PublicKey) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if rawKey.X == nil {
		return errors.Errorf(`invalid ecdsa.PublicKey`)
	}

	if rawKey.Y == nil {
		return errors.Errorf(`invalid ecdsa.PublicKey`)
	}

	xbuf := ecutil.AllocECPointBuffer(rawKey.X, rawKey.Curve)
	ybuf := ecutil.AllocECPointBuffer(rawKey.Y, rawKey.Curve)
	defer ecutil.ReleaseECPointBuffer(xbuf)
	defer ecutil.ReleaseECPointBuffer(ybuf)

	k.x = make([]byte, len(xbuf))
	copy(k.x, xbuf)
	k.y = make([]byte, len(ybuf))
	copy(k.y, ybuf)

	var crv jwa.EllipticCurveAlgorithm
	if tmp, ok := ecutil.AlgorithmForCurve(rawKey.Curve); ok {
		crv = tmp
	} else {
		return errors.Errorf(`invalid elliptic curve %s`, rawKey.Curve)
	}
	k.crv = &crv

	return nil
}

func (k *ecdsaPrivateKey) FromRaw(rawKey *ecdsa.PrivateKey) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if rawKey.PublicKey.X == nil {
		return errors.Errorf(`invalid ecdsa.PrivateKey`)
	}
	if rawKey.PublicKey.Y == nil {
		return errors.Errorf(`invalid ecdsa.PrivateKey`)
	}
	if rawKey.D == nil {
		return errors.Errorf(`invalid ecdsa.PrivateKey`)
	}

	xbuf := ecutil.AllocECPointBuffer(rawKey.PublicKey.X, rawKey.Curve)
	ybuf := ecutil.AllocECPointBuffer(rawKey.PublicKey.Y, rawKey.Curve)
	dbuf := ecutil.AllocECPointBuffer(rawKey.D, rawKey.Curve)
	defer ecutil.ReleaseECPointBuffer(xbuf)
	defer ecutil.ReleaseECPointBuffer(ybuf)
	defer ecutil.ReleaseECPointBuffer(dbuf)

	k.x = make([]byte, len(xbuf))
	copy(k.x, xbuf)
	k.y = make([]byte, len(ybuf))
	copy(k.y, ybuf)
	k.d = make([]byte, len(dbuf))
	copy(k.d, dbuf)

	var crv jwa.EllipticCurveAlgorithm
	if tmp, ok := ecutil.AlgorithmForCurve(rawKey.Curve); ok {
		crv = tmp
	} else {
		return errors.Errorf(`invalid elliptic curve %s`, rawKey.Curve)
	}
	k.crv = &crv

	return nil
}

func buildECDSAPublicKey(alg jwa.EllipticCurveAlgorithm, xbuf, ybuf []byte) (*ecdsa.PublicKey, error) {
	var crv elliptic.Curve
	if tmp, ok := ecutil.CurveForAlgorithm(alg); ok {
		crv = tmp
	} else {
		return nil, errors.Errorf(`invalid curve algorithm %s`, alg)
	}

	var x, y big.Int
	x.SetBytes(xbuf)
	y.SetBytes(ybuf)

	return &ecdsa.PublicKey{Curve: crv, X: &x, Y: &y}, nil
}

// Raw returns the EC-DSA public key represented by this JWK
func (k *ecdsaPublicKey) Raw(v interface{}) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	pubk, err := buildECDSAPublicKey(k.Crv(), k.x, k.y)
	if err != nil {
		return errors.Wrap(err, `failed to build public key`)
	}

	return blackmagic.AssignIfCompatible(v, pubk)
}

func (k *ecdsaPrivateKey) Raw(v interface{}) error {
	k.mu.RLock()
	defer k.mu.RUnlock()

	pubk, err := buildECDSAPublicKey(k.Crv(), k.x, k.y)
	if err != nil {
		return errors.Wrap(err, `failed to build public key`)
	}

	var key ecdsa.PrivateKey
	var d big.Int
	d.SetBytes(k.d)
	key.D = &d
	key.PublicKey = *pubk

	return blackmagic.AssignIfCompatible(v, &key)
}

func makeECDSAPublicKey(v interface {
	makePairs() []*HeaderPair
}) (Key, error) {
	newKey := NewECDSAPublicKey()

	// Iterate and copy everything except for the bits that should not be in the public key
	for _, pair := range v.makePairs() {
		switch pair.Key {
		case ECDSADKey:
			continue
		default:
			//nolint:forcetypeassert
			key := pair.Key.(string)
			if err := newKey.Set(key, pair.Value); err != nil {
				return nil, errors.Wrapf(err, `failed to set field %q`, key)
			}
		}
	}

	return newKey, nil
}

func (k *ecdsaPrivateKey) PublicKey() (Key, error) {
	return makeECDSAPublicKey(k)
}

func (k *ecdsaPublicKey) PublicKey() (Key, error) {
	return makeECDSAPublicKey(k)
}

func ecdsaThumbprint(hash crypto.Hash, crv, x, y string) []byte {
	h := hash.New()
	fmt.Fprint(h, `{"crv":"`)
	fmt.Fprint(h, crv)
	fmt.Fprint(h, `","kty":"EC","x":"`)
	fmt.Fprint(h, x)
	fmt.Fprint(h, `","y":"`)
	fmt.Fprint(h, y)
	fmt.Fprint(h, `"}`)
	return h.Sum(nil)
}

// Thumbprint returns the JWK thumbprint using the indicated
// hashing algorithm, according to RFC 7638
func (k ecdsaPublicKey) Thumbprint(hash crypto.Hash) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	var key ecdsa.PublicKey
	if err := k.Raw(&key); err != nil {
		return nil, errors.Wrap(err, `failed to materialize ecdsa.PublicKey for thumbprint generation`)
	}

	xbuf := ecutil.AllocECPointBuffer(key.X, key.Curve)
	ybuf := ecutil.AllocECPointBuffer(key.Y, key.Curve)
	defer ecutil.ReleaseECPointBuffer(xbuf)
	defer ecutil.ReleaseECPointBuffer(ybuf)

	return ecdsaThumbprint(
		hash,
		key.Curve.Params().Name,
		base64.EncodeToString(xbuf),
		base64.EncodeToString(ybuf),
	), nil
}

// Thumbprint returns the JWK thumbprint using the indicated
// hashing algorithm, according to RFC 7638
func (k ecdsaPrivateKey) Thumbprint(hash crypto.Hash) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	var key ecdsa.PrivateKey
	if err := k.Raw(&key); err != nil {
		return nil, errors.Wrap(err, `failed to materialize ecdsa.PrivateKey for thumbprint generation`)
	}

	xbuf := ecutil.AllocECPointBuffer(key.X, key.Curve)
	ybuf := ecutil.AllocECPointBuffer(key.Y, key.Curve)
	defer ecutil.ReleaseECPointBuffer(xbuf)
	defer ecutil.ReleaseECPointBuffer(ybuf)

	return ecdsaThumbprint(
		hash,
		key.Curve.Params().Name,
		base64.EncodeToString(xbuf),
		base64.EncodeToString(ybuf),
	), nil
}
