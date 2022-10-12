package jwe

import (
	"context"

	"github.com/lestrrat-go/jwx/internal/base64"
	"github.com/lestrrat-go/jwx/internal/json"

	"github.com/lestrrat-go/iter/mapiter"
	"github.com/lestrrat-go/jwx/internal/iter"
	"github.com/pkg/errors"
)

type isZeroer interface {
	isZero() bool
}

func (h *stdHeaders) isZero() bool {
	return h.agreementPartyUInfo == nil &&
		h.agreementPartyVInfo == nil &&
		h.algorithm == nil &&
		h.compression == nil &&
		h.contentEncryption == nil &&
		h.contentType == nil &&
		h.critical == nil &&
		h.ephemeralPublicKey == nil &&
		h.jwk == nil &&
		h.jwkSetURL == nil &&
		h.keyID == nil &&
		h.typ == nil &&
		h.x509CertChain == nil &&
		h.x509CertThumbprint == nil &&
		h.x509CertThumbprintS256 == nil &&
		h.x509URL == nil &&
		len(h.privateParams) == 0
}

// Iterate returns a channel that successively returns all the
// header name and values.
func (h *stdHeaders) Iterate(ctx context.Context) Iterator {
	pairs := h.makePairs()
	ch := make(chan *HeaderPair, len(pairs))
	go func(ctx context.Context, ch chan *HeaderPair, pairs []*HeaderPair) {
		defer close(ch)
		for _, pair := range pairs {
			select {
			case <-ctx.Done():
				return
			case ch <- pair:
			}
		}
	}(ctx, ch, pairs)
	return mapiter.New(ch)
}

func (h *stdHeaders) Walk(ctx context.Context, visitor Visitor) error {
	return iter.WalkMap(ctx, h, visitor)
}

func (h *stdHeaders) AsMap(ctx context.Context) (map[string]interface{}, error) {
	return iter.AsMap(ctx, h)
}

func (h *stdHeaders) Clone(ctx context.Context) (Headers, error) {
	dst := NewHeaders()
	if err := h.Copy(ctx, dst); err != nil {
		return nil, errors.Wrap(err, `failed to copy header contents to new object`)
	}
	return dst, nil
}

func (h *stdHeaders) Copy(ctx context.Context, dst Headers) error {
	for _, pair := range h.makePairs() {
		//nolint:forcetypeassert
		key := pair.Key.(string)
		if err := dst.Set(key, pair.Value); err != nil {
			return errors.Wrapf(err, `failed to set header %q`, key)
		}
	}
	return nil
}

func (h *stdHeaders) Merge(ctx context.Context, h2 Headers) (Headers, error) {
	h3 := NewHeaders()

	if h != nil {
		if err := h.Copy(ctx, h3); err != nil {
			return nil, errors.Wrap(err, `failed to copy headers from receiver`)
		}
	}

	if h2 != nil {
		if err := h2.Copy(ctx, h3); err != nil {
			return nil, errors.Wrap(err, `failed to copy headers from argument`)
		}
	}

	return h3, nil
}

func (h *stdHeaders) Encode() ([]byte, error) {
	buf, err := json.Marshal(h)
	if err != nil {
		return nil, errors.Wrap(err, `failed to marshal headers to JSON prior to encoding`)
	}

	return base64.Encode(buf), nil
}

func (h *stdHeaders) Decode(buf []byte) error {
	// base64 json string -> json object representation of header
	decoded, err := base64.Decode(buf)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal base64 encoded buffer")
	}

	if err := json.Unmarshal(decoded, h); err != nil {
		return errors.Wrap(err, "failed to unmarshal buffer")
	}

	return nil
}
