package jwt

import (
	"fmt"

	"github.com/lestrrat-go/jwx/internal/json"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwe"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/pkg/errors"
)

type SerializeCtx interface {
	Step() int
	Nested() bool
}

type serializeCtx struct {
	step   int
	nested bool
}

func (ctx *serializeCtx) Step() int {
	return ctx.step
}

func (ctx *serializeCtx) Nested() bool {
	return ctx.nested
}

type SerializeStep interface {
	Serialize(SerializeCtx, interface{}) (interface{}, error)
}

// Serializer is a generic serializer for JWTs. Whereas other conveinience
// functions can only do one thing (such as generate a JWS signed JWT),
// Using this construct you can serialize the token however you want.
//
// By default the serializer only marshals the token into a JSON payload.
// You must set up the rest of the steps that should be taken by the
// serializer.
//
// For example, to marshal the token into JSON, then apply JWS and JWE
// in that order, you would do:
//
//   serialized, err := jwt.NewSerialer().
//      Sign(jwa.RS256, key).
//      Encrypt(jwa.RSA_OAEP, key.PublicKey).
//      Serialize(token)
//
// The `jwt.Sign()` function is equivalent to
//
//   serialized, err := jwt.NewSerializer().
//      Sign(...args...).
//      Serialize(token)
type Serializer struct {
	steps []SerializeStep
}

// NewSerializer creates a new empty serializer.
func NewSerializer() *Serializer {
	return &Serializer{}
}

// Reset clears all of the registered steps.
func (s *Serializer) Reset() *Serializer {
	s.steps = nil
	return s
}

// Step adds a new Step to the serialization process
func (s *Serializer) Step(step SerializeStep) *Serializer {
	s.steps = append(s.steps, step)
	return s
}

type jsonSerializer struct{}

func (jsonSerializer) Serialize(_ SerializeCtx, v interface{}) (interface{}, error) {
	token, ok := v.(Token)
	if !ok {
		return nil, errors.Errorf(`invalid input: expected jwt.Token`)
	}

	buf, err := json.Marshal(token)
	if err != nil {
		return nil, errors.Errorf(`failed to serialize as JSON`)
	}
	return buf, nil
}

type genericHeader interface {
	Get(string) (interface{}, bool)
	Set(string, interface{}) error
}

func setTypeOrCty(ctx SerializeCtx, hdrs genericHeader) error {
	// cty and typ are common between JWE/JWS, so we don't use
	// the constants in jws/jwe package here
	const typKey = `typ`
	const ctyKey = `cty`

	if ctx.Step() == 1 {
		// We are executed immediately after json marshaling
		if _, ok := hdrs.Get(typKey); !ok {
			if err := hdrs.Set(typKey, `JWT`); err != nil {
				return errors.Wrapf(err, `failed to set %s key to "JWT"`, typKey)
			}
		}
	} else {
		if ctx.Nested() {
			// If this is part of a nested sequence, we should set cty = 'JWT'
			// https://datatracker.ietf.org/doc/html/rfc7519#section-5.2
			if err := hdrs.Set(ctyKey, `JWT`); err != nil {
				return errors.Wrapf(err, `failed to set %s key to "JWT"`, ctyKey)
			}
		}
	}
	return nil
}

type jwsSerializer struct {
	alg     jwa.SignatureAlgorithm
	key     interface{}
	options []SignOption
}

func (s *jwsSerializer) Serialize(ctx SerializeCtx, v interface{}) (interface{}, error) {
	payload, ok := v.([]byte)
	if !ok {
		return nil, errors.New(`expected []byte as input`)
	}

	var hdrs jws.Headers
	//nolint:forcetypeassert
	for _, option := range s.options {
		switch option.Ident() {
		case identJwsHeaders{}:
			hdrs = option.Value().(jws.Headers)
		}
	}

	if hdrs == nil {
		hdrs = jws.NewHeaders()
	}

	if err := setTypeOrCty(ctx, hdrs); err != nil {
		return nil, err // this is already wrapped
	}

	// JWTs MUST NOT use b64 = false
	// https://datatracker.ietf.org/doc/html/rfc7797#section-7
	if v, ok := hdrs.Get("b64"); ok {
		if bval, bok := v.(bool); bok {
			if !bval { // b64 = false
				return nil, errors.New(`b64 cannot be false for JWTs`)
			}
		}
	}
	return jws.Sign(payload, s.alg, s.key, jws.WithHeaders(hdrs))
}

func (s *Serializer) Sign(alg jwa.SignatureAlgorithm, key interface{}, options ...SignOption) *Serializer {
	return s.Step(&jwsSerializer{
		alg:     alg,
		key:     key,
		options: options,
	})
}

type jweSerializer struct {
	keyalg      jwa.KeyEncryptionAlgorithm
	key         interface{}
	contentalg  jwa.ContentEncryptionAlgorithm
	compressalg jwa.CompressionAlgorithm
	options     []EncryptOption
}

func (s *jweSerializer) Serialize(ctx SerializeCtx, v interface{}) (interface{}, error) {
	payload, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf(`expected []byte as input`)
	}

	var hdrs jwe.Headers
	//nolint:forcetypeassert
	for _, option := range s.options {
		switch option.Ident() {
		case identJweHeaders{}:
			hdrs = option.Value().(jwe.Headers)
		}
	}

	if hdrs == nil {
		hdrs = jwe.NewHeaders()
	}

	if err := setTypeOrCty(ctx, hdrs); err != nil {
		return nil, err // this is already wrapped
	}
	return jwe.Encrypt(payload, s.keyalg, s.key, s.contentalg, s.compressalg, jwe.WithProtectedHeaders(hdrs))
}

func (s *Serializer) Encrypt(keyalg jwa.KeyEncryptionAlgorithm, key interface{}, contentalg jwa.ContentEncryptionAlgorithm, compressalg jwa.CompressionAlgorithm, options ...EncryptOption) *Serializer {
	return s.Step(&jweSerializer{
		keyalg:      keyalg,
		key:         key,
		contentalg:  contentalg,
		compressalg: compressalg,
		options:     options,
	})
}

func (s *Serializer) Serialize(t Token) ([]byte, error) {
	steps := make([]SerializeStep, len(s.steps)+1)
	steps[0] = jsonSerializer{}
	for i, step := range s.steps {
		steps[i+1] = step
	}

	var ctx serializeCtx
	ctx.nested = len(s.steps) > 1
	var payload interface{} = t
	for i, step := range steps {
		ctx.step = i
		v, err := step.Serialize(&ctx, payload)
		if err != nil {
			return nil, errors.Wrapf(err, `failed to serialize token at step #%d`, i+1)
		}
		payload = v
	}

	res, ok := payload.([]byte)
	if !ok {
		return nil, errors.New(`invalid serialization produced`)
	}

	return res, nil
}
