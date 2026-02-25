// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package secrets

import (
	"context"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ISecretResolver resolves a secret reference (with the provider prefix
// already stripped) into the actual secret value. Each resolver owns its
// provider-specific configuration (project, prefix, credentials).
type ISecretResolver interface {
	// Resolve retrieves the secret value for the given reference.
	Resolve(ctx context.Context, reference string) (string, error)
	// Write creates or updates a secret and returns the resource reference
	// (without provider prefix). The resolver uses its own project/prefix
	// configuration to determine where to write.
	Write(ctx context.Context, secretID, value string) (reference string, err error)
	// Verify checks that a secret reference is readable.
	Verify(ctx context.Context, reference string) error
}

// Registry dispatches secret resolution based on the provider prefix of the
// reference string. For example, "gcp:projects/p/secrets/s/versions/1" routes
// to the resolver registered under "gcp". References without a recognized
// prefix are treated as static (literal) values.
//
// All methods are safe for concurrent use.
type Registry struct {
	mu        syncutil.RWMutex
	resolvers map[string]ISecretResolver
}

// NewRegistry creates an empty resolver registry.
func NewRegistry() *Registry {
	return &Registry{resolvers: make(map[string]ISecretResolver)}
}

// Register adds a resolver for the given provider prefix (e.g. "gcp").
func (r *Registry) Register(prefix string, resolver ISecretResolver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolvers[prefix] = resolver
}

// getResolver returns the resolver for the given prefix, if any.
func (r *Registry) getResolver(prefix string) (ISecretResolver, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	resolver, ok := r.resolvers[prefix]
	return resolver, ok
}

// Resolve splits the reference on the first ":" to extract the provider prefix,
// then delegates to the registered resolver. If no prefix is present or the
// prefix is unrecognized, the reference is returned as-is (static behavior).
func (r *Registry) Resolve(ctx context.Context, reference string) (string, error) {
	prefix, rest, hasSep := strings.Cut(reference, ":")
	if !hasSep {
		return reference, nil
	}

	resolver, ok := r.getResolver(prefix)
	if !ok {
		return reference, nil
	}

	val, err := resolver.Resolve(ctx, rest)
	if err != nil {
		return "", errors.Wrapf(err, "resolve secret (provider=%s)", prefix)
	}
	return val, nil
}

// Write creates or updates a secret via the resolver registered under the
// given prefix and returns the fully prefixed reference (e.g. "gcp:<ref>").
func (r *Registry) Write(ctx context.Context, prefix, secretID, value string) (string, error) {
	resolver, ok := r.getResolver(prefix)
	if !ok {
		return "", errors.Newf("no resolver registered for prefix %q", prefix)
	}
	ref, err := resolver.Write(ctx, secretID, value)
	if err != nil {
		return "", errors.Wrapf(err, "write secret (provider=%s)", prefix)
	}
	return prefix + ":" + ref, nil
}

// CanWrite returns true if a write-capable resolver is registered under
// the given prefix.
func (r *Registry) CanWrite(prefix string) bool {
	_, ok := r.getResolver(prefix)
	return ok
}

// Verify checks that a secret reference is readable by dispatching to the
// appropriate resolver. References without a recognized prefix are treated
// as literals and always pass verification.
func (r *Registry) Verify(ctx context.Context, reference string) error {
	prefix, rest, hasSep := strings.Cut(reference, ":")
	if !hasSep {
		return nil
	}
	resolver, ok := r.getResolver(prefix)
	if !ok {
		return nil
	}
	if err := resolver.Verify(ctx, rest); err != nil {
		return errors.Wrapf(err, "verify secret (provider=%s)", prefix)
	}
	return nil
}

// HasResolver returns true if the reference string starts with a prefix
// that matches a registered resolver.
func (r *Registry) HasResolver(reference string) bool {
	prefix, _, hasSep := strings.Cut(reference, ":")
	if !hasSep {
		return false
	}
	_, ok := r.getResolver(prefix)
	return ok
}

// Close releases resources held by registered resolvers. Resolvers that
// implement io.Closer have their Close method called.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, resolver := range r.resolvers {
		if closer, ok := resolver.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}
