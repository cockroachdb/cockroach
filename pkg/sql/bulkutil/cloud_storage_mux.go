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

// CloudStorageMux is a utility for managing multiple cloud storage instances.
// The main motivator for this is each node has its own nodelocal://<node-id>
// instance.
type CloudStorageMux struct {
	factory        cloud.ExternalStorageFromURIFactory
	storeInstances map[url.URL]cloud.ExternalStorage
	user           username.SQLUsername
}

func NewCloudStorageMux(
	factory cloud.ExternalStorageFromURIFactory, user username.SQLUsername,
) *CloudStorageMux {
	return &CloudStorageMux{
		factory:        factory,
		storeInstances: make(map[url.URL]cloud.ExternalStorage),
		user:           user,
	}
}

func (c *CloudStorageMux) Close() error {
	var err error
	for _, store := range c.storeInstances {
		err = errors.CombineErrors(err, store.Close())
	}
	return err
}

func (c *CloudStorageMux) StoreFile(ctx context.Context, uri string) (storageccl.StoreFile, error) {
	prefix, filepath, err := c.splitURI(uri)
	if err != nil {
		return storageccl.StoreFile{}, err
	}
	store, ok := c.storeInstances[prefix]
	if !ok {
		storage, err := c.factory(ctx, prefix.String(), c.user)
		if err != nil {
			return storageccl.StoreFile{}, err
		}
		c.storeInstances[prefix] = storage
		store = storage
	}
	return storageccl.StoreFile{
		Store:    store,
		FilePath: filepath,
	}, nil
}

func (c *CloudStorageMux) splitURI(uri string) (url.URL, string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return url.URL{}, "", errors.Wrap(err, "failed to parse external storage uri")
	}

	path := parsed.Path
	parsed.Path = ""

	return *parsed, path, nil
}
