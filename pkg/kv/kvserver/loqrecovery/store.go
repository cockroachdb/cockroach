// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

const recoveryPlanDir = "loq-recovery-plans"
const planFileName = "staged.bin"

type PlanStore struct {
	path string
	fs   vfs.FS
}

// NewPlanStore creates a plan store for recovery.
func NewPlanStore(path string, fs vfs.FS) PlanStore {
	return PlanStore{
		path: fs.PathJoin(path, recoveryPlanDir),
		fs:   fs,
	}
}

func (s PlanStore) RemovePlan() error {
	fileName := s.planFullName(planFileName)
	if err := s.fs.Remove(fileName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s PlanStore) SavePlan(plan loqrecoverypb.ReplicaUpdatePlan) error {
	if err := s.ensureStoreExists(); err != nil {
		return err
	}

	planBaseName := s.planFullName(planFileName)
	tmpFileName := planBaseName + ".tmp"
	defer func() { _ = s.fs.Remove(tmpFileName) }()

	if err := func() error {
		outFile, err := s.fs.Create(tmpFileName, fs.UnspecifiedWriteCategory)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %q", tmpFileName)
		}
		defer func() { _ = outFile.Close() }()
		var out []byte
		if out, err = protoutil.Marshal(&plan); err != nil {
			return errors.Wrap(err, "failed to marshal recovery plan")
		}
		if _, err = outFile.Write(out); err != nil {
			return errors.Wrap(err, "failed to write recovery plan")
		}
		if err = outFile.Sync(); err != nil {
			return errors.Wrap(err, "failed to sync recovery plan to disk")
		}
		return nil
	}(); err != nil {
		return err
	}

	if err := s.RemovePlan(); err != nil {
		return errors.Wrapf(err, "failed to remove previous plan")
	}
	if err := s.fs.Rename(tmpFileName, planBaseName); err != nil {
		return errors.Wrap(err, "failed to rename temp plan file")
	}
	return nil
}

func (s PlanStore) LoadPlan() (loqrecoverypb.ReplicaUpdatePlan, bool, error) {
	fileName := s.planFullName(planFileName)
	f, err := s.fs.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return loqrecoverypb.ReplicaUpdatePlan{}, false, nil
		}
		return loqrecoverypb.ReplicaUpdatePlan{}, false, errors.Wrapf(err, "failed to open plan file %q",
			fileName)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, false, errors.Wrapf(err, "failed to read plan file %q",
			fileName)
	}
	var nodeUpdates loqrecoverypb.ReplicaUpdatePlan
	if err = protoutil.Unmarshal(data, &nodeUpdates); err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, false, errors.Wrapf(err,
			"failed to unmarshal plan from file %q", fileName)
	}
	return nodeUpdates, true, nil
}

func (s PlanStore) planFullName(base string) string {
	return s.fs.PathJoin(s.path, base)
}

func (s PlanStore) ensureStoreExists() error {
	return errors.Wrapf(s.fs.MkdirAll(s.path, 0755),
		"failed to create recovery plan store directory %s",
		s.path)
}
