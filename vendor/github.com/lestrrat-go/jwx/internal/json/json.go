package json

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/lestrrat-go/jwx/internal/base64"
	"github.com/pkg/errors"
)

var muGlobalConfig sync.RWMutex
var useNumber bool

// Sets the global configuration for json decoding
func DecoderSettings(inUseNumber bool) {
	muGlobalConfig.Lock()
	useNumber = inUseNumber
	muGlobalConfig.Unlock()
}

// Unmarshal respects the values specified in DecoderSettings,
// and uses a Decoder that has certain features turned on/off
func Unmarshal(b []byte, v interface{}) error {
	dec := NewDecoder(bytes.NewReader(b))
	return dec.Decode(v)
}

func AssignNextBytesToken(dst *[]byte, dec *Decoder) error {
	var val string
	if err := dec.Decode(&val); err != nil {
		return errors.Wrap(err, `error reading next value`)
	}

	buf, err := base64.DecodeString(val)
	if err != nil {
		return errors.Errorf(`expected base64 encoded []byte (%T)`, val)
	}
	*dst = buf
	return nil
}

func ReadNextStringToken(dec *Decoder) (string, error) {
	var val string
	if err := dec.Decode(&val); err != nil {
		return "", errors.Wrap(err, `error reading next value`)
	}
	return val, nil
}

func AssignNextStringToken(dst **string, dec *Decoder) error {
	val, err := ReadNextStringToken(dec)
	if err != nil {
		return err
	}
	*dst = &val
	return nil
}

// FlattenAudience is a flag to specify if we should flatten the "aud"
// entry to a string when there's only one entry.
// In jwx < 1.1.8 we just dumped everything as an array of strings,
// but apparently AWS Cognito doesn't handle this well.
//
// So now we have the ability to dump "aud" as a string if there's
// only one entry, but we need to retain the old behavior so that
// we don't accidentally break somebody else's code. (e.g. messing
// up how signatures are calculated)
var FlattenAudience uint32

func EncodeAudience(enc *Encoder, aud []string) error {
	var val interface{}
	if len(aud) == 1 && atomic.LoadUint32(&FlattenAudience) == 1 {
		val = aud[0]
	} else {
		val = aud
	}
	return enc.Encode(val)
}

// DecodeCtx is an interface for objects that needs that extra something
// when decoding JSON into an object.
type DecodeCtx interface {
	Registry() *Registry
}

// DecodeCtxContainer is used to differentiate objects that can carry extra
// decoding hints and those who can't.
type DecodeCtxContainer interface {
	DecodeCtx() DecodeCtx
	SetDecodeCtx(DecodeCtx)
}

// stock decodeCtx. should cover 80% of the cases
type decodeCtx struct {
	registry *Registry
}

func NewDecodeCtx(r *Registry) DecodeCtx {
	return &decodeCtx{registry: r}
}

func (dc *decodeCtx) Registry() *Registry {
	return dc.registry
}
