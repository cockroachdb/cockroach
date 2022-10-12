package keygen

import (
	"crypto/ecdsa"

	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/x25519"
)

type Generator interface {
	Size() int
	Generate() (ByteSource, error)
}

// StaticKeyGenerate uses a static byte buffer to provide keys.
type Static []byte

// RandomKeyGenerate generates random keys
type Random struct {
	keysize int
}

// EcdhesKeyGenerate generates keys using ECDH-ES algorithm / EC-DSA curve
type Ecdhes struct {
	pubkey    *ecdsa.PublicKey
	keysize   int
	algorithm jwa.KeyEncryptionAlgorithm
	enc       jwa.ContentEncryptionAlgorithm
}

// X25519KeyGenerate generates keys using ECDH-ES algorithm / X25519 curve
type X25519 struct {
	algorithm jwa.KeyEncryptionAlgorithm
	enc       jwa.ContentEncryptionAlgorithm
	keysize   int
	pubkey    x25519.PublicKey
}

// ByteKey is a generated key that only has the key's byte buffer
// as its instance data. If a key needs to do more, such as providing
// values to be set in a JWE header, that key type wraps a ByteKey
type ByteKey []byte

// ByteWithECPublicKey holds the EC private key that generated
// the key along with the key itself. This is required to set the
// proper values in the JWE headers
type ByteWithECPublicKey struct {
	ByteKey
	PublicKey interface{}
}

type ByteWithIVAndTag struct {
	ByteKey
	IV  []byte
	Tag []byte
}

type ByteWithSaltAndCount struct {
	ByteKey
	Salt  []byte
	Count int
}

// ByteSource is an interface for things that return a byte sequence.
// This is used for KeyGenerator so that the result of computations can
// carry more than just the generate byte sequence.
type ByteSource interface {
	Bytes() []byte
}

type Setter interface {
	Set(string, interface{}) error
}
