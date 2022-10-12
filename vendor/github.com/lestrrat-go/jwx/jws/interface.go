package jws

import (
	"github.com/lestrrat-go/iter/mapiter"
	"github.com/lestrrat-go/jwx/internal/iter"
	"github.com/lestrrat-go/jwx/jwa"
)

type DecodeCtx interface {
	CollectRaw() bool
}

// Message represents a full JWS encoded message. Flattened serialization
// is not supported as a struct, but rather it's represented as a
// Message struct with only one `signature` element.
//
// Do not expect to use the Message object to verify or construct a
// signed payload with. You should only use this when you want to actually
// programmatically view the contents of the full JWS payload.
//
// As of this version, there is one big incompatibility when using Message
// objects to convert between compact and JSON representations.
// The protected header is sometimes encoded differently from the original
// message and the JSON serialization that we use in Go.
//
// For example, the protected header `eyJ0eXAiOiJKV1QiLA0KICJhbGciOiJIUzI1NiJ9`
// decodes to
//
//   {"typ":"JWT",
//     "alg":"HS256"}
//
// However, when we parse this into a message, we create a jws.Header object,
// which, when we marshal into a JSON object again, becomes
//
//   {"typ":"JWT","alg":"HS256"}
//
// Notice that serialization lacks a line break and a space between `"JWT",`
// and `"alg"`. This causes a problem when verifying the signatures AFTER
// a compact JWS message has been unmarshaled into a jws.Message.
//
// jws.Verify() doesn't go through this step, and therefore this does not
// manifest itself. However, you may see this discrepancy when you manually
// go through these conversions, and/or use the `jwx` tool like so:
//
//   jwx jws parse message.jws | jwx jws verify --key somekey.jwk --stdin
//
// In this scenario, the first `jwx jws parse` outputs a parsed jws.Message
// which is marshaled into JSON. At this point the message's protected
// headers and the signatures don't match.
//
// To sign and verify, use the appropriate `Sign()` and `Verify()` functions.
type Message struct {
	dc         DecodeCtx
	payload    []byte
	signatures []*Signature
	b64        bool // true if payload should be base64 encoded
}

type Signature struct {
	dc        DecodeCtx
	headers   Headers // Unprotected Headers
	protected Headers // Protected Headers
	signature []byte  // Signature
	detached  bool
}

type Visitor = iter.MapVisitor
type VisitorFunc = iter.MapVisitorFunc
type HeaderPair = mapiter.Pair
type Iterator = mapiter.Iterator

// Signer generates the signature for a given payload.
type Signer interface {
	// Sign creates a signature for the given payload.
	// The scond argument is the key used for signing the payload, and is usually
	// the private key type associated with the signature method. For example,
	// for `jwa.RSXXX` and `jwa.PSXXX` types, you need to pass the
	// `*"crypto/rsa".PrivateKey` type.
	// Check the documentation for each signer for details
	Sign([]byte, interface{}) ([]byte, error)

	Algorithm() jwa.SignatureAlgorithm
}

type hmacSignFunc func([]byte, []byte) ([]byte, error)

// HMACSigner uses crypto/hmac to sign the payloads.
type HMACSigner struct {
	alg  jwa.SignatureAlgorithm
	sign hmacSignFunc
}

type Verifier interface {
	// Verify checks whether the payload and signature are valid for
	// the given key.
	// `key` is the key used for verifying the payload, and is usually
	// the public key associated with the signature method. For example,
	// for `jwa.RSXXX` and `jwa.PSXXX` types, you need to pass the
	// `*"crypto/rsa".PublicKey` type.
	// Check the documentation for each verifier for details
	Verify(payload []byte, signature []byte, key interface{}) error
}

type HMACVerifier struct {
	signer Signer
}
