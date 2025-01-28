// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/storage/configpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
)

const PersistedConfigFilename = "STORAGE_CONFIG"

// TODO(baptist): Write to a temporary name first and then rename. Specifically
// reimplement the SafeWriteToFile to delay the rename until all stores have the
// new content.
func (p *Pebble) PersistNodeStoreConfig(ctx context.Context, contents configpb.Storage) error {
	vfs := p.cfg.env.UnencryptedFS
	filename := vfs.PathJoin(p.cfg.env.Dir, PersistedConfigFilename)

	b, err := contents.ToJson(true)
	if err != nil {
		return err
	}
	f, err := vfs.Create(filename, fs.UnspecifiedWriteCategory)
	if err != nil {
		return err
	}
	defer f.Close()
	bReader := bytes.NewReader(b)
	if _, err = io.Copy(f, bReader); err != nil {
		return err
	}
	return nil
}

func LoadNodeStoreConfig(
	ctx context.Context, bootstrapFile string,
) (configpb.Storage, bool, error) {
	f, err := os.OpenFile(bootstrapFile, os.O_RDONLY, 0)
	if err != nil {
		// Its not an error if the file doesn't exist.
		//nolint:returnerrcheck
		return configpb.Storage{}, false, nil
	}
	defer f.Close()
	return loadNodeStoreConfig(ctx, f)
}

func (p *Pebble) LoadNodeStoreConfig(ctx context.Context) (configpb.Storage, bool, error) {
	vfs := p.cfg.env.UnencryptedFS
	filename := vfs.PathJoin(p.cfg.env.Dir, PersistedConfigFilename)
	f, err := p.cfg.env.Open(filename)
	if err != nil {
		// Its not an error if the file doesn't exist.
		//nolint:returnerrcheck
		return configpb.Storage{}, false, nil
	}
	defer f.Close()
	return loadNodeStoreConfig(ctx, f)
}

// LoadNodeStoreConfig loads the bootstrap file from the given directory. It
// returns the config if there is no error and a bool which specifies whether
// the file was found.
func loadNodeStoreConfig(ctx context.Context, r io.Reader) (configpb.Storage, bool, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return configpb.Storage{}, true, err
	}

	config, err := configpb.FromJson(b)
	if err != nil {
		return configpb.Storage{}, true, err
	}
	return config, true, nil
}
