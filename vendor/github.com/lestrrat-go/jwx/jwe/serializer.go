package jwe

import (
	"context"

	"github.com/lestrrat-go/jwx/internal/base64"
	"github.com/lestrrat-go/jwx/internal/json"

	"github.com/lestrrat-go/jwx/internal/pool"
	"github.com/pkg/errors"
)

// Compact encodes the given message into a JWE compact serialization format.
//
// Currently `Compact()` does not take any options, but the API is
// set up as such to allow future expansions
func Compact(m *Message, _ ...SerializerOption) ([]byte, error) {
	if len(m.recipients) != 1 {
		return nil, errors.New("wrong number of recipients for compact serialization")
	}

	recipient := m.recipients[0]

	// The protected header must be a merge between the message-wide
	// protected header AND the recipient header

	// There's something wrong if m.protectedHeaders is nil, but
	// it could happen
	if m.protectedHeaders == nil {
		return nil, errors.New("invalid protected header")
	}

	ctx := context.TODO()
	hcopy, err := m.protectedHeaders.Clone(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to copy protected header")
	}
	hcopy, err = hcopy.Merge(ctx, m.unprotectedHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "failed to merge unprotected header")
	}
	hcopy, err = hcopy.Merge(ctx, recipient.Headers())
	if err != nil {
		return nil, errors.Wrap(err, "failed to merge recipient header")
	}

	protected, err := hcopy.Encode()
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode header")
	}

	encryptedKey := base64.Encode(recipient.EncryptedKey())
	iv := base64.Encode(m.initializationVector)
	cipher := base64.Encode(m.cipherText)
	tag := base64.Encode(m.tag)

	buf := pool.GetBytesBuffer()
	defer pool.ReleaseBytesBuffer(buf)

	buf.Grow(len(protected) + len(encryptedKey) + len(iv) + len(cipher) + len(tag) + 4)
	buf.Write(protected)
	buf.WriteByte('.')
	buf.Write(encryptedKey)
	buf.WriteByte('.')
	buf.Write(iv)
	buf.WriteByte('.')
	buf.Write(cipher)
	buf.WriteByte('.')
	buf.Write(tag)

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// JSON encodes the message into a JWE JSON serialization format.
//
// If `WithPrettyFormat(true)` is passed as an option, the returned
// value will be formatted using `json.MarshalIndent()`
func JSON(m *Message, options ...SerializerOption) ([]byte, error) {
	var pretty bool
	for _, option := range options {
		//nolint:forcetypeassert
		switch option.Ident() {
		case identPrettyFormat{}:
			pretty = option.Value().(bool)
		}
	}

	if pretty {
		return json.MarshalIndent(m, "", "  ")
	}
	return json.Marshal(m)
}
