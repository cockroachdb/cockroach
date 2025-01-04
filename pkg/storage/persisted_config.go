package storage

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/cockroachdb/cockroach/pkg/storage/configpb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
)

const PersistedConfigFilename = "STORAGE_CONFIG"

// TODO(baptist): Write to a temporary name first and then rename. Specifically
// reimplement the SafeWriteToFile to delay the rename until all stores have the
// new content.
func (p *Pebble) PersistNodeStoreConfig(ctx context.Context, contents configpb.Storage) error {
	vfs := p.cfg.env.UnencryptedFS
	dir := p.cfg.env.Dir
	filename := vfs.PathJoin(dir, PersistedConfigFilename)

	b, err := json.Marshal(&contents)
	if err != nil {
		return err
	}

	if err := fs.SafeWriteToFile(vfs, dir, filename, b, fs.UnspecifiedWriteCategory); err != nil {
		return err
	}
	return nil
}

// The bool is whether the file previously existed.
func (p *Pebble) LoadNodeStoreConfig(ctx context.Context) (configpb.Storage, bool, error) {
	vfs := p.cfg.env.UnencryptedFS
	dir := p.cfg.env.Dir

	f, err := vfs.Open(vfs.PathJoin(dir, PersistedConfigFilename))
	if err != nil {
		//nolint:returnerrcheck
		return configpb.Storage{}, false, nil
	}
	defer f.Close()
	return loadNodeStoreConfig(ctx, f)
}

// This is used against the bootstrap file before we have read any configs.
func LoadNodeStoreConfig(ctx context.Context, bootstrapDir string) (configpb.Storage, bool, error) {
	filename := filepath.Join(bootstrapDir, PersistedConfigFilename)
	f, err := os.OpenFile(filename, os.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		// Its not an error if the file doesn't exist.
		//nolint:returnerrcheck
		return configpb.Storage{}, false, nil
	}
	defer f.Close()
	return loadNodeStoreConfig(ctx, f)
}

func loadNodeStoreConfig(ctx context.Context, f io.Reader) (configpb.Storage, bool, error) {
	b, err := io.ReadAll(f)
	if err != nil {
		return configpb.Storage{}, true, err
	}
	config := configpb.Storage{}
	if err := json.Unmarshal(b, &config); err != nil {
		return configpb.Storage{}, true, err
	}
	return config, true, nil
}
