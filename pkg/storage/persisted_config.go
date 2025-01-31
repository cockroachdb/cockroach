// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/gogo/protobuf/jsonpb"
)

const PersistedConfigFilename = "STORAGE_CONFIG"

// TODO(baptist): Write to a temporary name first and then rename. Specifically
// reimplement the SafeWriteToFile to delay the rename until all stores have the
// new content.
func (p *Pebble) PersistNodeStoreConfig(ctx context.Context, contents storagepb.NodeConfig) error {
	vfs := p.cfg.env.UnencryptedFS
	filename := vfs.PathJoin(p.cfg.env.Dir, PersistedConfigFilename)

	b, err := toJson(contents, true)
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

func LoadNodeStoreConfig(bootstrapFile string) (storagepb.NodeConfig, bool, error) {
	f, err := os.OpenFile(bootstrapFile, os.O_RDONLY, 0)
	if err != nil {
		// Its not an error if the file doesn't exist.
		//nolint:returnerrcheck
		return storagepb.NodeConfig{}, false, nil
	}
	defer f.Close()
	return loadNodeStoreConfig(f)
}

func (p *Pebble) LoadNodeStoreConfig() (storagepb.NodeConfig, bool, error) {
	vfs := p.cfg.env.UnencryptedFS
	filename := vfs.PathJoin(p.cfg.env.Dir, PersistedConfigFilename)
	f, err := p.cfg.env.Open(filename)
	if err != nil {
		// Its not an error if the file doesn't exist.
		//nolint:returnerrcheck
		return storagepb.NodeConfig{}, false, nil
	}
	defer f.Close()
	return loadNodeStoreConfig(f)
}

// LoadNodeStoreConfig loads the bootstrap file from the given directory. It
// returns the config if there is no error and a bool which specifies whether
// the file was found.
func loadNodeStoreConfig(r io.Reader) (storagepb.NodeConfig, bool, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return storagepb.NodeConfig{}, true, err
	}

	config, err := fromJson(b)
	if err != nil {
		return storagepb.NodeConfig{}, true, err
	}
	return config, true, nil
}

// TODO: Use something like NestedText
// It is a little tricky to get the conversion to/from JSON correctly.
// Specifically it doesn't currently work with oneof in the proto. This is a
// convenience to make it clear which library is used.
func toJson(nc storagepb.NodeConfig, emitDefaults bool) ([]byte, error) {
	marshaller := jsonpb.Marshaler{
		Indent:       "\t",
		EmitDefaults: emitDefaults,
	}
	var buf bytes.Buffer
	err := marshaller.Marshal(&buf, &nc)
	return buf.Bytes(), err
}

func fromJson(b []byte) (storagepb.NodeConfig, error) {
	var nc storagepb.NodeConfig
	marshaller := jsonpb.Unmarshaler{}
	err := marshaller.Unmarshal(bytes.NewReader(b), &nc)
	return nc, err
}

// IsCompatible compares the config which was derived from the users parameters
// with the config that was persisted to disk.
// TODO: Where should this live?
func IsCompatible(nc, o storagepb.NodeConfig) error {
	// Create maps of stores for easy comparison.
	sMap := make(map[string]storagepb.StoreSpec)
	for _, store := range nc.Stores {
		sMap[store.Path] = store
	}

	oMap := make(map[string]storagepb.StoreSpec)
	for _, store := range o.Stores {
		oMap[store.Path] = store
	}

	// We only do a one-way validation from the persisted config to the user
	// config. If the persisted config has an incompatible change we return an
	// error.
	for path, oStore := range oMap {
		sStore, exists := sMap[path]
		if !exists {
			return incompatibleError(fmt.Sprintf("store %s removed", path), nc, o)
		}
		// If the store is explicitly marked as removed in the configuration, we
		// don't check the store. This is a manual attempt to remove the store.
		if sStore.State == storagepb.StoreState_REMOVED {
			continue
		}

		// TODO(baptist): We allow some encryption changes, figure out what is
		// acceptable and handle them correctly.
		if sStore.Encryption != oStore.Encryption {
			return incompatibleError("encryption changed", nc, o)
		}
	}

	return nil
}

// IncompatibleConfigError represents an error due to incompatible configuration changes.
type IncompatibleConfigError struct {
	Reason string
	Old    storagepb.NodeConfig
	New    storagepb.NodeConfig
}

func (e *IncompatibleConfigError) Error() string {
	old, err := toJson(e.Old, false)
	if err != nil {
		return fmt.Sprintf("failed to marshal old config: %v", err)
	}
	new, err := toJson(e.New, false)
	if err != nil {
		return fmt.Sprintf("failed to marshal new config: %v", err)
	}
	return fmt.Sprintf("incompatible config change %s\n%v \n%v", e.Reason, string(old[:]), string(new[:]))
}

func incompatibleError(reason string, o, nc storagepb.NodeConfig) error {
	return &IncompatibleConfigError{
		Reason: reason,
		Old:    nc,
		New:    o,
	}
}
