// This file is auto-generated. DO NOT EDIT

package jwk

import (
	"context"
	"crypto"
	"crypto/x509"

	"github.com/lestrrat-go/jwx/jwa"
)

const (
	KeyTypeKey                = "kty"
	KeyUsageKey               = "use"
	KeyOpsKey                 = "key_ops"
	AlgorithmKey              = "alg"
	KeyIDKey                  = "kid"
	X509URLKey                = "x58"
	X509CertChainKey          = "x5c"
	X509CertThumbprintKey     = "x5t"
	X509CertThumbprintS256Key = "x5t#S256"
)

// Key defines the minimal interface for each of the
// key types. Their use and implementation differ significantly
// between each key types, so you should use type assertions
// to perform more specific tasks with each key
type Key interface {
	// Get returns the value of a single field. The second boolean return value
	// will be false if the field is not stored in the source
	//
	// This method, which returns an `interface{}`, exists because
	// these objects can contain extra _arbitrary_ fields that users can
	// specify, and there is no way of knowing what type they could be
	Get(string) (interface{}, bool)

	// Set sets the value of a single field. Note that certain fields,
	// notably "kty", cannot be altered, but will not return an error
	//
	// This method, which takes an `interface{}`, exists because
	// these objects can contain extra _arbitrary_ fields that users can
	// specify, and there is no way of knowing what type they could be
	Set(string, interface{}) error

	// Remove removes the field associated with the specified key.
	// There is no way to remove the `kty` (key type). You will ALWAYS be left with one field in a jwk.Key.
	Remove(string) error

	// Raw creates the corresponding raw key. For example,
	// EC types would create *ecdsa.PublicKey or *ecdsa.PrivateKey,
	// and OctetSeq types create a []byte key.
	//
	// If you do not know the exact type of a jwk.Key before attempting
	// to obtain the raw key, you can simply pass a pointer to an
	// empty interface as the first argument.
	//
	// If you already know the exact type, it is recommended that you
	// pass a pointer to the zero value of the actual key type (e.g. &rsa.PrivateKey)
	// for efficiency.
	Raw(interface{}) error

	// Thumbprint returns the JWK thumbprint using the indicated
	// hashing algorithm, according to RFC 7638
	Thumbprint(crypto.Hash) ([]byte, error)

	// Iterate returns an iterator that returns all keys and values.
	// See github.com/lestrrat-go/iter for a description of the iterator.
	Iterate(ctx context.Context) HeaderIterator

	// Walk is a utility tool that allows a visitor to iterate all keys and values
	Walk(context.Context, HeaderVisitor) error

	// AsMap is a utility tool that returns a new map that contains the same fields as the source
	AsMap(context.Context) (map[string]interface{}, error)

	// PrivateParams returns the non-standard elements in the source structure
	// WARNING: DO NOT USE PrivateParams() IF YOU HAVE CONCURRENT CODE ACCESSING THEM.
	// Use `AsMap()` to get a copy of the entire header, or use `Iterate()` instead
	PrivateParams() map[string]interface{}

	// Clone creates a new instance of the same type
	Clone() (Key, error)

	// PublicKey creates the corresponding PublicKey type for this object.
	// All fields are copied onto the new public key, except for those that are not allowed.
	//
	// If the key is already a public key, it returns a new copy minus the disallowed fields as above.
	PublicKey() (Key, error)

	// KeyType returns the `kid` of a JWK
	KeyType() jwa.KeyType
	// KeyUsage returns `use` of a JWK
	KeyUsage() string
	// KeyOps returns `key_ops` of a JWK
	KeyOps() KeyOperationList
	// Algorithm returns `alg` of a JWK
	Algorithm() string
	// KeyID returns `kid` of a JWK
	KeyID() string
	// X509URL returns `x58` of a JWK
	X509URL() string
	// X509CertChain returns `x5c` of a JWK
	X509CertChain() []*x509.Certificate
	// X509CertThumbprint returns `x5t` of a JWK
	X509CertThumbprint() string
	// X509CertThumbprintS256 returns `x5t#S256` of a JWK
	X509CertThumbprintS256() string

	makePairs() []*HeaderPair
}
