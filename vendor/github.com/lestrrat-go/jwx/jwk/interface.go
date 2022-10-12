package jwk

import (
	"context"
	"crypto/x509"
	"net/http"
	"sync"

	"github.com/lestrrat-go/iter/arrayiter"
	"github.com/lestrrat-go/iter/mapiter"
	"github.com/lestrrat-go/jwx/internal/iter"
	"github.com/lestrrat-go/jwx/internal/json"
)

// KeyUsageType is used to denote what this key should be used for
type KeyUsageType string

const (
	// ForSignature is the value used in the headers to indicate that
	// this key should be used for signatures
	ForSignature KeyUsageType = "sig"
	// ForEncryption is the value used in the headers to indicate that
	// this key should be used for encrypting
	ForEncryption KeyUsageType = "enc"
)

type CertificateChain struct {
	certs []*x509.Certificate
}

type KeyOperation string
type KeyOperationList []KeyOperation

const (
	KeyOpSign       KeyOperation = "sign"       // (compute digital signature or MAC)
	KeyOpVerify     KeyOperation = "verify"     // (verify digital signature or MAC)
	KeyOpEncrypt    KeyOperation = "encrypt"    // (encrypt content)
	KeyOpDecrypt    KeyOperation = "decrypt"    // (decrypt content and validate decryption, if applicable)
	KeyOpWrapKey    KeyOperation = "wrapKey"    // (encrypt key)
	KeyOpUnwrapKey  KeyOperation = "unwrapKey"  // (decrypt key and validate decryption, if applicable)
	KeyOpDeriveKey  KeyOperation = "deriveKey"  // (derive key)
	KeyOpDeriveBits KeyOperation = "deriveBits" // (derive bits not to be used as a key)
)

// Set represents JWKS object, a collection of jwk.Key objects.
//
// Sets can be safely converted to and from JSON using the standard
// `"encoding/json".Marshal` and `"encoding/json".Unmarshal`. However,
// if you do not know if the payload contains a single JWK or a JWK set,
// consider using `jwk.Parse()` to always get a `jwk.Set` out of it.
//
// Since v1.2.12, JWK sets with private parameters can be parsed as well.
// Such private parameters can be accessed via the `Field()` method.
// If a resource contains a single JWK instead of a JWK set, private parameters
// are stored in _both_ the resulting `jwk.Set` object and the `jwk.Key` object .
//
type Set interface {
	// Add adds the specified key. If the key already exists in the set, it is
	// not added.
	// This method will be renamed to `AddKey(Key)` in a future major release.
	Add(Key) bool

	// Clear resets the list of keys associated with this set, emptying the
	// internal list of `jwk.Key`s
	// This method will be changed in the future to clear all contents in the
	// `jwk.Set` instead of just the keys.
	Clear()

	// Get returns the key at index `idx`. If the index is out of range,
	// then the second return value is false.
	// This method will be renamed to `Key(int)` in a future major release.
	Get(int) (Key, bool)

	// Field returns the value of a private field in the key set.
	//
	// For the purposes of a key set, any field other than the "keys" field is
	// considered to be a private field. In other words, you cannot use this
	// method to directly access the list of keys in the set
	//
	// This method will be renamed to `Get(string)` in a future major release.
	Field(string) (interface{}, bool)

	// Set sets the value of a single field.
	//
	// This method, which takes an `interface{}`, exists because
	// these objects can contain extra _arbitrary_ fields that users can
	// specify, and there is no way of knowing what type they could be.
	Set(string, interface{}) error

	// Remove removes the field associated with the specified key.
	// There is no way to remove the `kty` (key type). You will ALWAYS be left with one field in a jwk.Key.
	// Index returns the index where the given key exists, -1 otherwise
	Index(Key) int

	// Len returns the number of keys in the set
	Len() int

	// LookupKeyID returns the first key matching the given key id.
	// The second return value is false if there are no keys matching the key id.
	// The set *may* contain multiple keys with the same key id. If you
	// need all of them, use `Iterate()`
	LookupKeyID(string) (Key, bool)

	// Remove removes the key from the set.
	Remove(Key) bool

	// Iterate creates an iterator to iterate through all keys in the set.
	Iterate(context.Context) KeyIterator

	// Clone create a new set with identical keys. Keys themselves are not cloned.
	Clone() (Set, error)
}

type set struct {
	keys          []Key
	mu            sync.RWMutex
	dc            DecodeCtx
	privateParams map[string]interface{}
}

type HeaderVisitor = iter.MapVisitor
type HeaderVisitorFunc = iter.MapVisitorFunc
type HeaderPair = mapiter.Pair
type HeaderIterator = mapiter.Iterator
type KeyPair = arrayiter.Pair
type KeyIterator = arrayiter.Iterator

type PublicKeyer interface {
	// PublicKey creates the corresponding PublicKey type for this object.
	// All fields are copied onto the new public key, except for those that are not allowed.
	// Returned value must not be the receiver itself.
	PublicKey() (Key, error)
}

// HTTPClient specifies the minimum interface that is required for our JWK
// fetching tools.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type DecodeCtx interface {
	json.DecodeCtx
	IgnoreParseError() bool
}
type KeyWithDecodeCtx interface {
	SetDecodeCtx(DecodeCtx)
	DecodeCtx() DecodeCtx
}

type AutoRefreshError struct {
	Error error
	URL   string
}

// Whitelist is an interface for a set of URL whitelists. When provided
// to JWK fetching operations, urls are checked against this object, and
// the object must return true for urls to be fetched.
type Whitelist interface {
	IsAllowed(string) bool
}
