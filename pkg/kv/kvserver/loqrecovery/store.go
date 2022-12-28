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
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

const recoveryPlanDir = "loq-recovery-plans"
const extension = ".json"

type PlanStore struct {
	path string
	fs   vfs.FS
}

type StoredPlan struct {
	PlanID       uuid.UUID
	CreationTime time.Time
}

func (p StoredPlan) Empty() bool {
	return p.PlanID.Equal(uuid.UUID{})
}

func (p StoredPlan) SamePlan(planID uuid.UUID) bool {
	return planID.Equal(p.PlanID)
}

// NewPlanStore creates a plan store for recovery.
func NewPlanStore(path string, fs vfs.FS) PlanStore {
	return PlanStore{
		path: fs.PathJoin(path, recoveryPlanDir),
		fs:   fs,
	}
}

func (s PlanStore) HasPlan() (StoredPlan, error) {
	infos, err := s.listPlanFiles()
	if err != nil {
		return StoredPlan{}, err
	}
	if len(infos) > 1 {
		return StoredPlan{}, errors.Newf("more than a single recovery plan exists in store: %s, %s",
			s.fs.PathJoin(s.path, infos[0].Name()),
			s.fs.PathJoin(s.path, infos[1].Name()))
	}
	if len(infos) > 0 {
		name := infos[0].Name()
		planID, err := uuid.FromString(name[:len(name)-len(extension)])
		if err != nil {
			return StoredPlan{}, errors.Newf("found corrupted file name in loq recovery store: %s",
				s.fs.PathJoin(s.path, infos[0].Name()))
		}
		return StoredPlan{
			PlanID:       planID,
			CreationTime: infos[0].ModTime(),
		}, nil
	}
	return StoredPlan{}, nil
}

func (s PlanStore) RemovePlans() error {
	infos, err := s.listPlanFiles()
	if err != nil {
		return err
	}
	for _, i := range infos {
		if err := s.fs.Remove(s.fs.PathJoin(s.path, i.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s PlanStore) SavePlan(plan loqrecoverypb.ReplicaUpdatePlan) error {
	existing, err := s.HasPlan()
	if err != nil {
		return errors.Wrap(err, "failed to perform existing recovery plan check")
	}
	if existing.SamePlan(plan.PlanID) {
		// Plan is already set, nothing to do.
		return nil
	}
	if !existing.Empty() {
		return errors.Newf("can not set recovery plan %s, plan %s is already staged", plan.PlanID,
			existing.PlanID)
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
		if out, err = jsonpb.Marshal(&plan); err != nil {
			return errors.Wrap(err, "failed to marshal recovery plan")
		}
		if _, err = outFile.Write(out); err != nil {
			return errors.Wrap(err, "failed to write recovery plan")
		}
		return nil
	}(); err != nil {
		return err
	}

	if err := s.fs.Rename(tmpFileName, planBaseName+extension); err != nil {
		_ = s.fs.Remove(tmpFileName)
		return errors.Wrap(err, "failed to rename temp plan file")
	}
	return nil
}

func (s PlanStore) LoadPlan(plan StoredPlan) (loqrecoverypb.ReplicaUpdatePlan, error) {
	fileName := s.planFullName(plan.PlanID.String() + extension)
	f, err := s.fs.Open(fileName)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Wrapf(err, "failed to open plan file %q",
			fileName)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Wrapf(err, "failed to read plan file %q",
			fileName)
	}

	var nodeUpdates loqrecoverypb.ReplicaUpdatePlan
	jsonpb := protoutil.JSONPb{Indent: "  "}
	if err = jsonpb.Unmarshal(data, &nodeUpdates); err != nil {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Wrapf(err,
			"failed to unmarshal plan from file %q", fileName)
	}

	if !plan.SamePlan(nodeUpdates.PlanID) {
		return loqrecoverypb.ReplicaUpdatePlan{}, errors.Newf("loaded plan has id %s while filename is %s, plan id must match filename without extension",
			nodeUpdates.PlanID, plan.PlanID)
	}

	return nodeUpdates, nil
}

func (s PlanStore) listPlanFiles() ([]fs.FileInfo, error) {
	names, err := s.fs.List(s.path)
	if err != nil {
		// Error means we can't access recovery dir. That means no plans are
		// available.
		return nil, nil //nolint:returnerrcheck
	}
	var files []fs.FileInfo
	filePattern := regexp.MustCompile(`[a-zA-Z0-9-]*\.json`)
	for _, n := range names {
		if filePattern.MatchString(n) {
			info, err := s.fs.Stat(s.planFullName(n))
			if err != nil {
				return nil, errors.Newf("failed to get file stats for recovery plan file: %s",
					s.planFullName(info.Name()))
			}
			if info.IsDir() {
				continue
			}
			files = append(files, info)
		}
	}
	return files, nil
}

func (s PlanStore) planFullName(base string) string {
	return s.fs.PathJoin(s.path, base)
}

func (s PlanStore) ensureStoreExists() error {
	return errors.Wrapf(s.fs.MkdirAll(s.path, 0755),
		"failed to create recovery plan store directory %s",
		s.path)
}
