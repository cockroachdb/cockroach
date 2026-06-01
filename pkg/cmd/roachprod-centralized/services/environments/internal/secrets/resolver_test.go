// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package secrets

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// stubResolver is a test double that returns canned values.
type stubResolver struct {
	values    map[string]string
	written   map[string]string // track writes: secretID -> value
	verifyErr error             // if non-nil, Verify returns this
}

func (r *stubResolver) Resolve(_ context.Context, reference string) (string, error) {
	val, ok := r.values[reference]
	if !ok {
		return "", errors.Newf("not found: %s", reference)
	}
	return val, nil
}

func (r *stubResolver) Write(_ context.Context, secretID, value string) (string, error) {
	ref := fmt.Sprintf("projects/stub-project/secrets/%s/versions/latest", secretID)
	if r.written == nil {
		r.written = make(map[string]string)
	}
	r.written[secretID] = value
	return ref, nil
}

func (r *stubResolver) Verify(_ context.Context, reference string) error {
	if r.verifyErr != nil {
		return r.verifyErr
	}
	return nil
}

func TestRegistry_Resolve(t *testing.T) {
	ctx := context.Background()

	t.Run("dispatches to registered resolver", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{
			values: map[string]string{
				"projects/p/secrets/s/versions/1": "secret-value",
			},
		})

		val, err := reg.Resolve(ctx, "gcp:projects/p/secrets/s/versions/1")
		require.NoError(t, err)
		require.Equal(t, "secret-value", val)
	})

	t.Run("unrecognized prefix returns literal", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{})

		val, err := reg.Resolve(ctx, "vault:some/path")
		require.NoError(t, err)
		require.Equal(t, "vault:some/path", val)
	})

	t.Run("no prefix returns literal", func(t *testing.T) {
		reg := NewRegistry()
		val, err := reg.Resolve(ctx, "plaintext-value")
		require.NoError(t, err)
		require.Equal(t, "plaintext-value", val)
	})

	t.Run("resolver error propagates", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{values: map[string]string{}})

		_, err := reg.Resolve(ctx, "gcp:nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "resolve secret")
	})

	t.Run("multiple resolvers", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{
			values: map[string]string{"ref1": "gcp-val"},
		})
		reg.Register("vault", &stubResolver{
			values: map[string]string{"ref2": "vault-val"},
		})

		val, err := reg.Resolve(ctx, "gcp:ref1")
		require.NoError(t, err)
		require.Equal(t, "gcp-val", val)

		val, err = reg.Resolve(ctx, "vault:ref2")
		require.NoError(t, err)
		require.Equal(t, "vault-val", val)
	})

	t.Run("colon in reference value", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{
			values: map[string]string{"projects/p/secrets/s:latest": "val"},
		})

		// "gcp:projects/p/secrets/s:latest" → prefix="gcp", rest="projects/p/secrets/s:latest"
		val, err := reg.Resolve(ctx, "gcp:projects/p/secrets/s:latest")
		require.NoError(t, err)
		require.Equal(t, "val", val)
	})
}

func TestRegistry_Write(t *testing.T) {
	ctx := context.Background()

	t.Run("writes via registered resolver", func(t *testing.T) {
		stub := &stubResolver{}
		reg := NewRegistry()
		reg.Register("gcp", stub)

		ref, err := reg.Write(ctx, "gcp", "my-secret", "val")
		require.NoError(t, err)
		require.Equal(t, "gcp:projects/stub-project/secrets/my-secret/versions/latest", ref)
		require.Equal(t, "val", stub.written["my-secret"])
	})

	t.Run("unregistered prefix returns error", func(t *testing.T) {
		reg := NewRegistry()

		_, err := reg.Write(ctx, "vault", "sec", "val")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no resolver registered")
	})
}

func TestRegistry_Verify(t *testing.T) {
	ctx := context.Background()

	t.Run("dispatches to registered resolver", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{})

		err := reg.Verify(ctx, "gcp:some/ref")
		require.NoError(t, err)
	})

	t.Run("propagates resolver error", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{
			verifyErr: errors.New("access denied"),
		})

		err := reg.Verify(ctx, "gcp:some/ref")
		require.Error(t, err)
		require.Contains(t, err.Error(), "access denied")
	})

	t.Run("no prefix returns nil", func(t *testing.T) {
		reg := NewRegistry()

		err := reg.Verify(ctx, "plaintext-value")
		require.NoError(t, err)
	})

	t.Run("unrecognized prefix returns nil", func(t *testing.T) {
		reg := NewRegistry()
		reg.Register("gcp", &stubResolver{})

		err := reg.Verify(ctx, "vault:some/ref")
		require.NoError(t, err)
	})
}

func TestRegistry_HasResolver(t *testing.T) {
	reg := NewRegistry()
	reg.Register("gcp", &stubResolver{})

	require.True(t, reg.HasResolver("gcp:ref"))
	require.False(t, reg.HasResolver("vault:ref"))
	require.False(t, reg.HasResolver("no-prefix"))
}
