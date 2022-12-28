// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

const RECOVERY_PLAN_DIR = "recovery-plans"

type PlanStorage struct {
	path string
	fs   vfs.FS
}

type StoredPlan struct {
	Name         string
	CreationTime time.Time
}

func (p StoredPlan) Empty() bool {
	return len(p.Name) == 0
}

// NewPlanStore creates a plan store for recovery.
func NewPlanStore(path string, fs vfs.FS) PlanStorage {
	return PlanStorage{
		path: filepath.Join(path, RECOVERY_PLAN_DIR),
		fs:   fs,
	}
}

func (s PlanStorage) HasPlan() (StoredPlan, error) {
	var (
		name    string
		created time.Time
	)
	if err := s.visitPlanFiles(func(info fs.FileInfo) error {
		if len(name) > 0 {
			return errors.Newf("more than a single recovery plan exists in store: %s, %s",
				filepath.Join(s.path, name), filepath.Join(s.path, info.Name()))
		}
		name = info.Name()
		created = info.ModTime()
		return nil
	}); err != nil {
		return StoredPlan{}, err
	}
	if len(name) > 0 {
		return StoredPlan{
			Name:         s.planFullName(name),
			CreationTime: created,
		}, nil
	}
	return StoredPlan{}, nil
}

func (s PlanStorage) RemovePlans() error {
	return s.visitPlanFiles(func(info fs.FileInfo) error {
		return s.fs.Remove(filepath.Join(s.path, info.Name()))
	})
}

func (s PlanStorage) SavePlan(plan loqrecoverypb.ReplicaUpdatePlan) error {
	existing, err := s.HasPlan()
	if err != nil {
		return errors.Wrap(err, "failed to perform existing recovery plan check")
	}
	if !existing.Empty() {
		return errors.Newf("can not set recovery plan %s, plan file %s is already set", plan.PlanID,
			existing.Name)
	}

	if err := s.ensureStoreExists(); err != nil {
		return err
	}

	planBaseName := s.planFullName(plan.PlanID.String())
	tmpFileName := planBaseName + ".tmp"
	defer func() { _ = s.fs.Remove(tmpFileName) }()

	if err := func() error {
		outFile, err := s.fs.Create(tmpFileName)
		if err != nil {
			return errors.Wrapf(err, "failed to create file %q", tmpFileName)
		}
		defer func() { _ = outFile.Close() }()
		jsonpb := protoutil.JSONPb{Indent: "  "}
		var out []byte
		if out, err = jsonpb.Marshal(plan); err != nil {
			return errors.Wrap(err, "failed to marshal recovery plan")
		}
		if _, err = outFile.Write(out); err != nil {
			return errors.Wrap(err, "failed to write recovery plan")
		}
		return nil
	}(); err != nil {
		return err
	}

	if err := s.fs.Rename(tmpFileName, planBaseName+".json"); err != nil {
		_ = s.fs.Remove(tmpFileName)
		return errors.Wrap(err, "failed to rename temp plan file")
	}
	return nil
}

func (s PlanStorage) LoadPlan(plan StoredPlan) (loqrecoverypb.ReplicaUpdatePlan, error) {
	f, err := s.fs.Open(plan.Name)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Wrapf(err, "failed to open plan file %q",
			plan.Name)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Wrapf(err, "failed to read plan file %q",
			plan.Name)
	}

	var nodeUpdates loqrecoverypb.ReplicaUpdatePlan
	jsonpb := protoutil.JSONPb{Indent: "  "}
	if err = jsonpb.Unmarshal(data, &nodeUpdates); err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Wrapf(err,
			"failed to unmarshal plan from file %q", plan.Name)
	}

	if s.planFullName(nodeUpdates.PlanID.String()+".json") != plan.Name {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Newf("loaded plan has id %s while filename is %s, plan id must match filename without extension",
			nodeUpdates.PlanID, plan.Name)
	}

	return nodeUpdates, nil
}

func (s PlanStorage) visitPlanFiles(visitor func(fs.FileInfo) error) error {
	names, err := s.fs.List(s.path)
	if err != nil {
		// Error means we can't access recovery dir. That means no plans are
		// available.
		return nil //nolint:returnerrcheck
	}
	filePattern := regexp.MustCompile(`[a-zA-Z0-9-]*\.json`)
	for _, n := range names {
		if filePattern.MatchString(n) {
			info, err := s.fs.Stat(s.planFullName(n))
			if err != nil {
				return errors.Newf("failed to get file stats for recovery plan file: %s",
					s.planFullName(info.Name()))
			}
			if info.IsDir() {
				continue
			}
			if err := visitor(info); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s PlanStorage) planFullName(base string) string {
	return filepath.Join(s.path, base)
}

func (s PlanStorage) ensureStoreExists() error {
	return errors.Wrapf(s.fs.MkdirAll(s.path, 0755),
		"failed to create recovery plan store directory %s",
		s.path)
}
