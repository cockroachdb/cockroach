// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

// ExternalStorageMux is a utility for managing multiple cloud storage instances.
// The main motivator for this is each node has its own nodelocal://<node-id>
// instance.
type ExternalStorageMux struct {
	factory        cloud.ExternalStorageFromURIFactory
	storeInstances map[string]cloud.ExternalStorage
	user           username.SQLUsername
}

// NewExternalStorageMux creates a new ExternalStorageMux that caches external storage
// instances. This is particularly useful for nodelocal storage where each node
// has its own storage instance (nodelocal://1/, nodelocal://2/, etc.).
func NewExternalStorageMux(
	factory cloud.ExternalStorageFromURIFactory, user username.SQLUsername,
) *ExternalStorageMux {
	return &ExternalStorageMux{
		factory:        factory,
		storeInstances: make(map[string]cloud.ExternalStorage),
		user:           user,
	}
}

// Close closes all cached storage instances.
func (c *ExternalStorageMux) Close() error {
	var err error
	for _, store := range c.storeInstances {
		err = errors.CombineErrors(err, store.Close())
	}
	return err
}

// StoreFile splits a URI into its storage prefix and file path, caching the
// storage instance for reuse. For example, "nodelocal://1/import/123/file.sst"
// is split into storage "nodelocal://1" and path "/import/123/file.sst".
func (c *ExternalStorageMux) StoreFile(
	ctx context.Context, uri string,
) (storageccl.StoreFile, error) {
	prefix, filepath, err := c.splitURI(uri)
	if err != nil {
		return storageccl.StoreFile{}, err
	}
	prefixKey := prefix.String()
	store, ok := c.storeInstances[prefixKey]
	if !ok {
		storage, err := c.factory(ctx, prefix.String(), c.user)
		if err != nil {
			return storageccl.StoreFile{}, err
		}
		c.storeInstances[prefixKey] = storage
		store = storage
	}
	return storageccl.StoreFile{
		Store:    store,
		FilePath: filepath,
	}, nil
}

// splitURI splits a URI into its prefix (scheme + host) and path components.
// For example, "nodelocal://1/import/123/file.sst" becomes:
//   - prefix: url.URL{Scheme: "nodelocal", Host: "1"}
//   - path: "/import/123/file.sst"
func (c *ExternalStorageMux) splitURI(uri string) (url.URL, string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return url.URL{}, "", errors.Wrap(err, "failed to parse external storage uri")
	}

	path := parsed.Path
	parsed.Path = ""

	return *parsed, path, nil
}
