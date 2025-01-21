// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package configpb

import (
	"bytes"
	"context"
	fmt "fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/gogo/protobuf/jsonpb"
)

// It is a little tricky to get the conversion to/from JSON correctly.
// Specifically it doesn't currently work with oneof in the proto. This is a
// convenience to make it clear which library is used.
func (s *Storage) ToJson(emitDefaults bool) ([]byte, error) {
	marshaller := jsonpb.Marshaler{
		Indent:       "\t",
		EmitDefaults: emitDefaults,
	}
	var buf bytes.Buffer
	err := marshaller.Marshal(&buf, s)
	return buf.Bytes(), err
}

func FromJson(b []byte) (Storage, error) {
	var s Storage
	marshaller := jsonpb.Unmarshaler{}
	err := marshaller.Unmarshal(bytes.NewReader(b), &s)
	return s, err
}

// The conversion to byte[] is legacy for when EncryptionOptions was under the ccl.
func (e *EncryptionOptions) AsBytes() []byte {
	if e == nil {
		return nil
	}
	b, err := protoutil.Marshal(e)
	if err != nil {
		panic(fmt.Sprintf("Unexpected failure of AsBytes %+v, %v", e, err))
	}
	return b
}

// PreventedStartupFile is the filename (relative to 'dir') used for files that
// can block server startup.
func PreventedStartupFile(dir string) string {
	return filepath.Join(dir, "_CRITICAL_ALERT.txt")
}

// PriorCriticalAlertError attempts to read the
// PreventedStartupFile for each store directory and returns their
// contents as a structured error.
//
// These files typically request operator intervention after a
// corruption event by preventing the affected node(s) from starting
// back up.
// TODO: Fixme to use configpb
func (s *Storage) PriorCriticalAlertError() (err error) {
	addError := func(newErr error) {
		if err == nil {
			err = errors.New("startup forbidden by prior critical alert")
		}
		// We use WithDetailf here instead of errors.CombineErrors
		// because we want the details to be printed to the screen
		// (combined errors only show up via %+v).
		err = errors.WithDetailf(err, "%v", newErr)
	}
	for _, ss := range s.Stores {
		path := ss.PreventedStartupFile()
		if path == "" {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			if !oserror.IsNotExist(err) {
				addError(errors.Wrapf(err, "%s", path))
			}
			continue
		}
		addError(errors.Newf("From %s:\n\n%s\n", path, b))
	}
	return err
}

// EmergencyBallastFile returns the path (relative to a data directory) used
// for an emergency ballast file. The returned path must be stable across
// releases (eg, we cannot change these constants), otherwise we may duplicate
// ballasts.
func EmergencyBallastFile(pathJoin func(...string) string, dataDir string) string {
	return pathJoin(dataDir, AuxiliaryDir, "EMERGENCY_BALLAST")
}

// AuxiliaryDir is the path of the auxiliary dir relative to an engine.Engine's
// root directory. It must not be changed without a proper migration.
const AuxiliaryDir = "auxiliary"

// PreventedStartupFile returns the path to a file which, if it exists, should
// prevent the server from starting up. Returns an empty string for in-memory
// engines.
func (s Store) PreventedStartupFile() string {
	if s.InMemory {
		return ""
	}
	return PreventedStartupFile(filepath.Join(s.Path, AuxiliaryDir))
}

// IsCompatible compares the config which was derived from the users parameters
// with the config that was persisted to disk.
func (s *Storage) IsCompatible(ctx context.Context, o *Storage) error {
	// Create maps of stores for easy comparison.
	sMap := make(map[string]Store)
	for _, store := range s.Stores {
		sMap[store.Path] = store
	}

	oMap := make(map[string]Store)
	for _, store := range o.Stores {
		oMap[store.Path] = store
	}

	// We only do a one-way validation from the persisted config to the user
	// config. If the persisted config has an incompatible change we return an
	// error.
	for path, oStore := range oMap {
		sStore, exists := sMap[path]
		if !exists {
			return incompatibleError(fmt.Sprintf("store %s removed", path), s, o)
		}
		// If the store is explicitly marked as removed in the configuration, we
		// don't check the store. This is a manual attempt to remove the store.
		if sStore.State == StoreState_Removed {
			continue
		}

		// TODO(baptist): We allow some encryption changes, figure out what is
		// acceptable and handle them correctly.
		if sStore.Encryption != oStore.Encryption {
			return incompatibleError("encryption changed", s, o)
		}
	}

	return nil
}

// IncompatibleConfigError represents an error due to incompatible configuration changes.
type IncompatibleConfigError struct {
	Reason string
	Old    *Storage
	New    *Storage
}

func (e *IncompatibleConfigError) Error() string {
	old, err := e.Old.ToJson(false)
	if err != nil {
		return fmt.Sprintf("failed to marshal old config: %v", err)
	}
	new, err := e.New.ToJson(false)
	if err != nil {
		return fmt.Sprintf("failed to marshal new config: %v", err)
	}
	return fmt.Sprintf("incompatible config change %s\n%v \n%v", e.Reason, string(old[:]), string(new[:]))
}

func incompatibleError(reason string, s, o *Storage) error {
	return &IncompatibleConfigError{
		Reason: reason,
		Old:    s,
		New:    o,
	}
}
