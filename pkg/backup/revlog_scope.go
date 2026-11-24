// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// revlogScope is the production revlogjob.Scope, answering the
// writer's "what should I cover?" / "does this change matter?" /
// "have my roots dissolved?" questions in terms of the parent
// BACKUP's resolved targets — captured at job creation and never
// re-resolved by name. A DROP+CREATE that swaps a same-named DB
// to a different ID is a different scope, and would need a
// different revlog (created by a future BACKUP run).
//
// Cluster mode (FullCluster=true): in scope = every descriptor in
// the tenant minus the system DB and the cluster-backup
// exclusion set. Targets mode: in scope = ID ∈ resolved or
// ParentID ∈ expandedDBs.
//
// DROP-state descriptors keep matching (we want the schema-delta
// stream to capture the transition) but their spans drop out of
// the resolved set returned by Spans — a forward RESTORE
// wouldn't include the dropped table anyway, so capturing the
// GC tombstones that follow is pure cost.
type revlogScope struct {
	execCfg     *sql.ExecutorConfig
	fullCluster bool
	resolved    map[descpb.ID]struct{}
	expandedDBs map[descpb.ID]struct{}
	description string

	// systemExclusions is the cluster-mode exclusion set, loaded
	// lazily on the first Matches/Spans call (the lookup needs a
	// transaction so we can't do it in the constructor).
	systemExclusions map[descpb.ID]struct{}
}

var _ revlogjob.Scope = (*revlogScope)(nil)

// newRevlogScope builds a Scope from BackupDetails.
func newRevlogScope(execCfg *sql.ExecutorConfig, details jobspb.BackupDetails) *revlogScope {
	s := &revlogScope{
		execCfg:     execCfg,
		fullCluster: details.FullCluster,
		description: describeScope(details),
	}
	if !details.FullCluster {
		s.resolved = make(map[descpb.ID]struct{}, len(details.ResolvedTargets))
		for i := range details.ResolvedTargets {
			d := &details.ResolvedTargets[i]
			s.resolved[descIDOf(d)] = struct{}{}
		}
		s.expandedDBs = make(map[descpb.ID]struct{}, len(details.ResolvedCompleteDbs))
		for _, id := range details.ResolvedCompleteDbs {
			s.expandedDBs[id] = struct{}{}
		}
	}
	return s
}

// describeScope renders BackupDetails as a short string for
// embedding in Coverage.Scope. Inspection only.
func describeScope(details jobspb.BackupDetails) string {
	if details.FullCluster {
		return "cluster"
	}
	var ids []string
	for _, id := range details.ResolvedCompleteDbs {
		ids = append(ids, fmt.Sprintf("db:%d", id))
	}
	for i := range details.ResolvedTargets {
		ids = append(ids, fmt.Sprintf("desc:%d", descIDOf(&details.ResolvedTargets[i])))
	}
	return "targets:" + strings.Join(ids, ",")
}

// descIDOf extracts the ID from a Descriptor without unwrapping
// to a typed catalog.Descriptor.
func descIDOf(d *descpb.Descriptor) descpb.ID {
	tbl, db, typ, sch, fn := descpb.GetDescriptors(d)
	switch {
	case tbl != nil:
		return tbl.GetID()
	case db != nil:
		return db.GetID()
	case typ != nil:
		return typ.GetID()
	case sch != nil:
		return sch.GetID()
	case fn != nil:
		return fn.GetID()
	}
	return descpb.InvalidID
}

// descParentIDOf returns the descriptor's parent (database) ID,
// or InvalidID for descriptors with no parent (databases
// themselves; some system-internal types).
func descParentIDOf(d *descpb.Descriptor) descpb.ID {
	tbl, _, typ, sch, fn := descpb.GetDescriptors(d)
	switch {
	case tbl != nil:
		return tbl.GetParentID()
	case typ != nil:
		return typ.GetParentID()
	case sch != nil:
		return sch.GetParentID()
	case fn != nil:
		return fn.GetParentID()
	}
	return descpb.InvalidID
}

func (s *revlogScope) Matches(desc *descpb.Descriptor) bool {
	if desc == nil {
		return false
	}
	id := descIDOf(desc)
	if id == descpb.InvalidID {
		return false
	}
	if s.fullCluster {
		if id == keys.SystemDatabaseID {
			return false
		}
		if _, excluded := s.systemExclusions[id]; excluded {
			return false
		}
		return true
	}
	if _, ok := s.resolved[id]; ok {
		return true
	}
	// A database descriptor's own changes (CREATE, DROP, RENAME)
	// matter when the database is in expandedDBs, even though its
	// ParentID is InvalidID and the children-of-expanded-DB clause
	// below wouldn't catch it.
	if _, ok := s.expandedDBs[id]; ok {
		return true
	}
	if parent := descParentIDOf(desc); parent != descpb.InvalidID {
		if _, ok := s.expandedDBs[parent]; ok {
			return true
		}
	}
	return false
}

func (s *revlogScope) String() string { return s.description }

func (s *revlogScope) Spans(ctx context.Context, asOf hlc.Timestamp) ([]roachpb.Span, error) {
	tables, err := s.matchingTablesAt(ctx, asOf)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}
	return spansForAllTableIndexes(s.execCfg, tables, nil /* revs */)
}

func (s *revlogScope) Terminated(ctx context.Context, asOf hlc.Timestamp) (bool, error) {
	if s.fullCluster {
		return false, nil
	}
	allDescs, err := backupresolver.LoadAllDescs(ctx, s.execCfg, asOf)
	if err != nil {
		return false, errors.Wrap(err, "loading descriptors for termination check")
	}
	live := make(map[descpb.ID]bool, len(allDescs))
	for _, d := range allDescs {
		if d.Dropped() {
			continue
		}
		live[d.GetID()] = true
	}
	for id := range s.resolved {
		if live[id] {
			return false, nil
		}
	}
	for id := range s.expandedDBs {
		if live[id] {
			return false, nil
		}
	}
	return true, nil
}

// matchingTablesAt returns the non-DROP TableDescriptors at asOf
// that match the scope predicate, lazily filling systemExclusions
// the first time it runs in cluster mode.
func (s *revlogScope) matchingTablesAt(
	ctx context.Context, asOf hlc.Timestamp,
) ([]catalog.TableDescriptor, error) {
	if s.fullCluster && s.systemExclusions == nil {
		excl, err := GetSystemTableIDsToExcludeFromClusterBackup(ctx, s.execCfg, asOf)
		if err != nil {
			return nil, errors.Wrap(err, "loading cluster-backup system exclusions")
		}
		s.systemExclusions = excl
	}
	allDescs, err := backupresolver.LoadAllDescs(ctx, s.execCfg, asOf)
	if err != nil {
		return nil, errors.Wrap(err, "loading descriptors")
	}
	var tables []catalog.TableDescriptor
	for _, d := range allDescs {
		t, ok := d.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		if t.Dropped() {
			continue
		}
		// Reuse Matches by going via the proto. cheap.
		if !s.Matches(t.DescriptorProto()) {
			continue
		}
		tables = append(tables, t)
	}
	return tables, nil
}
