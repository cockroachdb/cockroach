package jws

import (
	"context"

	"github.com/lestrrat-go/iter/mapiter"
	"github.com/lestrrat-go/jwx/internal/iter"
	"github.com/pkg/errors"
)

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

// mergeHeaders merges two headers, and works even if the first Header
// object is nil. This is not exported because ATM it felt like this
// function is not frequently used, and MergeHeaders seemed a clunky name
func mergeHeaders(ctx context.Context, h1, h2 Headers) (Headers, error) {
	h3 := NewHeaders()

	if h1 != nil {
		if err := h1.Copy(ctx, h3); err != nil {
			return nil, errors.Wrap(err, `failed to copy headers from first Header`)
		}
	}

	if h2 != nil {
		if err := h2.Copy(ctx, h3); err != nil {
			return nil, errors.Wrap(err, `failed to copy headers from second Header`)
		}
	}

	return h3, nil
}

func (h *stdHeaders) Merge(ctx context.Context, h2 Headers) (Headers, error) {
	return mergeHeaders(ctx, h, h2)
}
