//go:generate ./gen.sh

// Package jwt implements JSON Web Tokens as described in https://tools.ietf.org/html/rfc7519
package jwt

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/lestrrat-go/backoff/v2"
	"github.com/lestrrat-go/jwx"
	"github.com/lestrrat-go/jwx/internal/json"
	"github.com/lestrrat-go/jwx/jwe"

	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/pkg/errors"
)

const _jwt = `jwt`

// Settings controls global settings that are specific to JWTs.
func Settings(options ...GlobalOption) {
	var flattenAudienceBool bool

	//nolint:forcetypeassert
	for _, option := range options {
		switch option.Ident() {
		case identFlattenAudience{}:
			flattenAudienceBool = option.Value().(bool)
		}
	}

	v := atomic.LoadUint32(&json.FlattenAudience)
	if (v == 1) != flattenAudienceBool {
		var newVal uint32
		if flattenAudienceBool {
			newVal = 1
		}
		atomic.CompareAndSwapUint32(&json.FlattenAudience, v, newVal)
	}
}

var registry = json.NewRegistry()

// ParseString calls Parse against a string
func ParseString(s string, options ...ParseOption) (Token, error) {
	return parseBytes([]byte(s), options...)
}

// Parse parses the JWT token payload and creates a new `jwt.Token` object.
// The token must be encoded in either JSON format or compact format.
//
// This function can work with encrypted and/or signed tokens. Any combination
// of JWS and JWE may be applied to the token, but this function will only
// attempt to verify/decrypt up to 2 levels (i.e. JWS only, JWE only, JWS then
// JWE, or JWE then JWS)
//
// If the token is signed and you want to verify the payload matches the signature,
// you must pass the jwt.WithVerify(alg, key) or jwt.WithKeySet(jwk.Set) option.
// If you do not specify these parameters, no verification will be performed.
//
// During verification, if the JWS headers specify a key ID (`kid`), the
// key used for verification must match the specified ID. If you are somehow
// using a key without a `kid` (which is highly unlikely if you are working
// with a JWT from a well know provider), you can workaround this by modifying
// the `jwk.Key` and setting the `kid` header.
//
// If you also want to assert the validity of the JWT itself (i.e. expiration
// and such), use the `Validate()` function on the returned token, or pass the
// `WithValidate(true)` option. Validate options can also be passed to
// `Parse`
//
// This function takes both ParseOption and ValidateOption types:
// ParseOptions control the parsing behavior, and ValidateOptions are
// passed to `Validate()` when `jwt.WithValidate` is specified.
func Parse(s []byte, options ...ParseOption) (Token, error) {
	return parseBytes(s, options...)
}

// ParseReader calls Parse against an io.Reader
func ParseReader(src io.Reader, options ...ParseOption) (Token, error) {
	// We're going to need the raw bytes regardless. Read it.
	data, err := ioutil.ReadAll(src)
	if err != nil {
		return nil, errors.Wrap(err, `failed to read from token data source`)
	}
	return parseBytes(data, options...)
}

type parseCtx struct {
	decryptParams    DecryptParameters
	verifyParams     VerifyParameters
	keySet           jwk.Set
	keySetProvider   KeySetProvider
	token            Token
	validateOpts     []ValidateOption
	verifyAutoOpts   []jws.VerifyOption
	localReg         *json.Registry
	inferAlgorithm   bool
	pedantic         bool
	skipVerification bool
	useDefault       bool
	validate         bool
	verifyAuto       bool
}

func parseBytes(data []byte, options ...ParseOption) (Token, error) {
	var ctx parseCtx
	for _, o := range options {
		if v, ok := o.(ValidateOption); ok {
			ctx.validateOpts = append(ctx.validateOpts, v)
			continue
		}

		//nolint:forcetypeassert
		switch o.Ident() {
		case identVerifyAuto{}:
			ctx.verifyAuto = o.Value().(bool)
		case identFetchWhitelist{}:
			ctx.verifyAutoOpts = append(ctx.verifyAutoOpts, jws.WithFetchWhitelist(o.Value().(jwk.Whitelist)))
		case identHTTPClient{}:
			ctx.verifyAutoOpts = append(ctx.verifyAutoOpts, jws.WithHTTPClient(o.Value().(*http.Client)))
		case identFetchBackoff{}:
			ctx.verifyAutoOpts = append(ctx.verifyAutoOpts, jws.WithFetchBackoff(o.Value().(backoff.Policy)))
		case identJWKSetFetcher{}:
			ctx.verifyAutoOpts = append(ctx.verifyAutoOpts, jws.WithJWKSetFetcher(o.Value().(jws.JWKSetFetcher)))
		case identVerify{}:
			ctx.verifyParams = o.Value().(VerifyParameters)
		case identDecrypt{}:
			ctx.decryptParams = o.Value().(DecryptParameters)
		case identKeySet{}:
			ks, ok := o.Value().(jwk.Set)
			if !ok {
				return nil, errors.Errorf(`invalid JWK set passed via WithKeySet() option (%T)`, o.Value())
			}
			ctx.keySet = ks
		case identToken{}:
			token, ok := o.Value().(Token)
			if !ok {
				return nil, errors.Errorf(`invalid token passed via WithToken() option (%T)`, o.Value())
			}
			ctx.token = token
		case identPedantic{}:
			ctx.pedantic = o.Value().(bool)
		case identDefault{}:
			ctx.useDefault = o.Value().(bool)
		case identValidate{}:
			ctx.validate = o.Value().(bool)
		case identTypedClaim{}:
			pair := o.Value().(claimPair)
			if ctx.localReg == nil {
				ctx.localReg = json.NewRegistry()
			}
			ctx.localReg.Register(pair.Name, pair.Value)
		case identInferAlgorithmFromKey{}:
			ctx.inferAlgorithm = o.Value().(bool)
		case identKeySetProvider{}:
			ctx.keySetProvider = o.Value().(KeySetProvider)
		}
	}

	data = bytes.TrimSpace(data)
	return parse(&ctx, data)
}

const (
	_JwsVerifyInvalid = iota
	_JwsVerifyDone
	_JwsVerifyExpectNested
	_JwsVerifySkipped
)

func verifyJWS(ctx *parseCtx, payload []byte) ([]byte, int, error) {
	if ctx.verifyAuto {
		options := ctx.verifyAutoOpts
		verified, err := jws.VerifyAuto(payload, options...)
		return verified, _JwsVerifyDone, err
	}

	// if we have a key set or a provider, use that
	ks := ctx.keySet
	p := ctx.keySetProvider
	if ks != nil || p != nil {
		return verifyJWSWithKeySet(ctx, payload)
	}

	// We can't proceed without verification parameters
	vp := ctx.verifyParams
	if vp == nil {
		return nil, _JwsVerifySkipped, nil
	}

	return verifyJWSWithParams(ctx, payload, vp.Algorithm(), vp.Key())
}

func verifyJWSWithKeySet(ctx *parseCtx, payload []byte) ([]byte, int, error) {
	// First, get the JWS message
	msg, err := jws.Parse(payload)
	if err != nil {
		return nil, _JwsVerifyInvalid, errors.Wrap(err, `failed to parse token data as JWS message`)
	}
	ks := ctx.keySet
	if ks == nil { // the caller should have checked ctx.keySet || ctx.keySetProvider
		if p := ctx.keySetProvider; p != nil {
			// "trust" the payload, and parse it so that the provider can do its thing
			ctx.skipVerification = true
			tok, err := parse(ctx, msg.Payload())
			if err != nil {
				return nil, _JwsVerifyInvalid, err
			}
			ctx.skipVerification = false

			v, err := p.KeySetFrom(tok)
			if err != nil {
				return nil, _JwsVerifyInvalid, errors.Wrap(err, `failed to obtain jwk.Set from KeySetProvider`)
			}
			ks = v
		}
	}

	// Bail out early if we don't even have a key in the set
	if ks.Len() == 0 {
		return nil, _JwsVerifyInvalid, errors.New(`empty keyset provided`)
	}

	var key jwk.Key

	// Find the kid. we need the kid, unless the user explicitly
	// specified to use the "default" (the first and only) key in the set
	headers := msg.Signatures()[0].ProtectedHeaders()
	kid := headers.KeyID()
	if kid == "" {
		// If the kid is NOT specified... ctx.useDefault needs to be true, and the
		// JWKs must have exactly one key in it
		if !ctx.useDefault {
			return nil, _JwsVerifyInvalid, errors.New(`failed to find matching key: no key ID ("kid") specified in token`)
		} else if ctx.useDefault && ks.Len() > 1 {
			return nil, _JwsVerifyInvalid, errors.New(`failed to find matching key: no key ID ("kid") specified in token but multiple keys available in key set`)
		}

		// if we got here, then useDefault == true AND there is exactly
		// one key in the set.
		key, _ = ks.Get(0)
	} else {
		// Otherwise we better be able to look up the key, baby.
		v, ok := ks.LookupKeyID(kid)
		if !ok {
			return nil, _JwsVerifyInvalid, errors.Errorf(`failed to find key with key ID %q in key set`, kid)
		}
		key = v
	}

	// We found a key with matching kid. Check fo the algorithm specified in the key.
	// If we find an algorithm in the key, use that.
	if v := key.Algorithm(); v != "" {
		var alg jwa.SignatureAlgorithm
		if err := alg.Accept(v); err != nil {
			return nil, _JwsVerifyInvalid, errors.Wrapf(err, `invalid signature algorithm %s`, key.Algorithm())
		}

		// Okay, we have a valid algorithm, go go
		return verifyJWSWithParams(ctx, payload, alg, key)
	}

	if ctx.inferAlgorithm {
		// Check whether the JWT headers specify a valid
		// algorithm, use it if it's compatible.
		algs, err := jws.AlgorithmsForKey(key)
		if err != nil {
			return nil, _JwsVerifyInvalid, errors.Wrapf(err, `failed to get a list of signature methods for key type %s`, key.KeyType())
		}

		for _, alg := range algs {
			// bail out if the JWT has a `alg` field, and it doesn't match
			if tokAlg := headers.Algorithm(); tokAlg != "" {
				if tokAlg != alg {
					continue
				}
			}

			return verifyJWSWithParams(ctx, payload, alg, key)
		}
	}

	return nil, _JwsVerifyInvalid, errors.New(`failed to match any of the keys`)
}

func verifyJWSWithParams(ctx *parseCtx, payload []byte, alg jwa.SignatureAlgorithm, key interface{}) ([]byte, int, error) {
	var m *jws.Message
	var verifyOpts []jws.VerifyOption
	if ctx.pedantic {
		m = jws.NewMessage()
		verifyOpts = []jws.VerifyOption{jws.WithMessage(m)}
	}
	v, err := jws.Verify(payload, alg, key, verifyOpts...)
	if err != nil {
		return nil, _JwsVerifyInvalid, errors.Wrap(err, `failed to verify jws signature`)
	}

	if !ctx.pedantic {
		return v, _JwsVerifyDone, nil
	}
	// This payload could be a JWT+JWS, in which case typ: JWT should be there
	// If its JWT+(JWE or JWS or...)+JWS, then cty should be JWT
	for _, sig := range m.Signatures() {
		hdrs := sig.ProtectedHeaders()
		if strings.ToLower(hdrs.Type()) == _jwt {
			return v, _JwsVerifyDone, nil
		}

		if strings.ToLower(hdrs.ContentType()) == _jwt {
			return v, _JwsVerifyExpectNested, nil
		}
	}

	// Hmmm, it was a JWS and we got... nothing?
	return nil, _JwsVerifyInvalid, errors.Errorf(`expected "typ" or "cty" fields, neither could be found`)
}

// verify parameter exists to make sure that we don't accidentally skip
// over verification just because alg == ""  or key == nil or something.
func parse(ctx *parseCtx, data []byte) (Token, error) {
	payload := data
	const maxDecodeLevels = 2

	// If cty = `JWT`, we expect this to be a nested structure
	var expectNested bool

OUTER:
	for i := 0; i < maxDecodeLevels; i++ {
		switch kind := jwx.GuessFormat(payload); kind {
		case jwx.JWT:
			if ctx.pedantic {
				if expectNested {
					return nil, errors.Errorf(`expected nested encrypted/signed payload, got raw JWT`)
				}
			}

			if i == 0 {
				// We were NOT enveloped in other formats
				if !ctx.skipVerification {
					if _, _, err := verifyJWS(ctx, payload); err != nil {
						return nil, err
					}
				}
			}

			break OUTER
		case jwx.UnknownFormat:
			// "Unknown" may include invalid JWTs, for example, those who lack "aud"
			// claim. We could be pedantic and reject these
			if ctx.pedantic {
				return nil, errors.Errorf(`invalid JWT`)
			}

			if i == 0 {
				// We were NOT enveloped in other formats
				if !ctx.skipVerification {
					if _, _, err := verifyJWS(ctx, payload); err != nil {
						return nil, err
					}
				}
			}
			break OUTER
		case jwx.JWS:
			// Food for thought: This is going to break if you have multiple layers of
			// JWS enveloping using different keys. It is highly unlikely use case,
			// but it might happen.

			// skipVerification should only be set to true by us. It's used
			// when we just want to parse the JWT out of a payload
			if !ctx.skipVerification {
				// nested return value means:
				// false (next envelope _may_ need to be processed)
				// true (next envelope MUST be processed)
				v, state, err := verifyJWS(ctx, payload)
				if err != nil {
					return nil, err
				}

				if state != _JwsVerifySkipped {
					payload = v

					// We only check for cty and typ if the pedantic flag is enabled
					if !ctx.pedantic {
						continue
					}

					if state == _JwsVerifyExpectNested {
						expectNested = true
						continue OUTER
					}

					// if we're not nested, we found our target. bail out of this loop
					break OUTER
				}
			}

			// No verification.
			m, err := jws.Parse(data)
			if err != nil {
				return nil, errors.Wrap(err, `invalid jws message`)
			}
			payload = m.Payload()
		case jwx.JWE:
			dp := ctx.decryptParams
			if dp == nil {
				return nil, errors.Errorf(`jwt.Parse: cannot proceed with JWE encrypted payload without decryption parameters`)
			}

			var m *jwe.Message
			var decryptOpts []jwe.DecryptOption
			if ctx.pedantic {
				m = jwe.NewMessage()
				decryptOpts = []jwe.DecryptOption{jwe.WithMessage(m)}
			}

			v, err := jwe.Decrypt(data, dp.Algorithm(), dp.Key(), decryptOpts...)
			if err != nil {
				return nil, errors.Wrap(err, `failed to decrypt payload`)
			}

			if !ctx.pedantic {
				payload = v
				continue
			}

			if strings.ToLower(m.ProtectedHeaders().Type()) == _jwt {
				payload = v
				break OUTER
			}

			if strings.ToLower(m.ProtectedHeaders().ContentType()) == _jwt {
				expectNested = true
				payload = v
				continue OUTER
			}
		default:
			return nil, errors.Errorf(`unsupported format (layer: #%d)`, i+1)
		}
		expectNested = false
	}

	if ctx.token == nil {
		ctx.token = New()
	}

	if ctx.localReg != nil {
		dcToken, ok := ctx.token.(TokenWithDecodeCtx)
		if !ok {
			return nil, errors.Errorf(`typed claim was requested, but the token (%T) does not support DecodeCtx`, ctx.token)
		}
		dc := json.NewDecodeCtx(ctx.localReg)
		dcToken.SetDecodeCtx(dc)
		defer func() { dcToken.SetDecodeCtx(nil) }()
	}

	if err := json.Unmarshal(payload, ctx.token); err != nil {
		return nil, errors.Wrap(err, `failed to parse token`)
	}

	if ctx.validate {
		if err := Validate(ctx.token, ctx.validateOpts...); err != nil {
			return nil, err
		}
	}
	return ctx.token, nil
}

// Sign is a convenience function to create a signed JWT token serialized in
// compact form.
//
// It accepts either a raw key (e.g. rsa.PrivateKey, ecdsa.PrivateKey, etc)
// or a jwk.Key, and the name of the algorithm that should be used to sign
// the token.
//
// If the key is a jwk.Key and the key contains a key ID (`kid` field),
// then it is added to the protected header generated by the signature
//
// The algorithm specified in the `alg` parameter must be able to support
// the type of key you provided, otherwise an error is returned.
//
// The protected header will also automatically have the `typ` field set
// to the literal value `JWT`, unless you provide a custom value for it
// by jwt.WithHeaders option.
func Sign(t Token, alg jwa.SignatureAlgorithm, key interface{}, options ...SignOption) ([]byte, error) {
	return NewSerializer().Sign(alg, key, options...).Serialize(t)
}

// Equal compares two JWT tokens. Do not use `reflect.Equal` or the like
// to compare tokens as they will also compare extra detail such as
// sync.Mutex objects used to control concurrent access.
//
// The comparison for values is currently done using a simple equality ("=="),
// except for time.Time, which uses time.Equal after dropping the monotonic
// clock and truncating the values to 1 second accuracy.
//
// if both t1 and t2 are nil, returns true
func Equal(t1, t2 Token) bool {
	if t1 == nil && t2 == nil {
		return true
	}

	// we already checked for t1 == t2 == nil, so safe to do this
	if t1 == nil || t2 == nil {
		return false
	}

	j1, err := json.Marshal(t1)
	if err != nil {
		return false
	}

	j2, err := json.Marshal(t2)
	if err != nil {
		return false
	}

	return bytes.Equal(j1, j2)
}

func (t *stdToken) Clone() (Token, error) {
	dst := New()

	for _, pair := range t.makePairs() {
		//nolint:forcetypeassert
		key := pair.Key.(string)
		if err := dst.Set(key, pair.Value); err != nil {
			return nil, errors.Wrapf(err, `failed to set %s`, key)
		}
	}
	return dst, nil
}

// RegisterCustomField allows users to specify that a private field
// be decoded as an instance of the specified type. This option has
// a global effect.
//
// For example, suppose you have a custom field `x-birthday`, which
// you want to represent as a string formatted in RFC3339 in JSON,
// but want it back as `time.Time`.
//
// In that case you would register a custom field as follows
//
//   jwt.RegisterCustomField(`x-birthday`, timeT)
//
// Then `token.Get("x-birthday")` will still return an `interface{}`,
// but you can convert its type to `time.Time`
//
//   bdayif, _ := token.Get(`x-birthday`)
//   bday := bdayif.(time.Time)
//
func RegisterCustomField(name string, object interface{}) {
	registry.Register(name, object)
}
