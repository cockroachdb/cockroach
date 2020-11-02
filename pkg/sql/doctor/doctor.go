// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package doctor provides utilities for checking the consistency of cockroach
// internal persisted metadata.
package doctor

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/sortkeys"
)

// DescriptorTableRow represents a descriptor from table `system.descriptor`.
type DescriptorTableRow struct {
	ID        int64
	DescBytes []byte
	ModTime   hlc.Timestamp
}

type DescriptorSet interface {
	NumDescriptors() int
	Iterate(func(id descpb.ID, desc catalog.Descriptor) error) error
	catalog.DescGetter
}

// DescriptorTable represents data read from `system.descriptor`.
type DescriptorTable struct {
	raw       []DescriptorTableRow
	unwrapped []catalog.Descriptor
	catalog.DescGetter
}

func (d DescriptorTable) NumDescriptors() int {
	return len(d.raw)
}

func (d DescriptorTable) Iterate(f func(id descpb.ID, desc catalog.Descriptor) error) error {
	for i, desc := range d.unwrapped {
		if err := f(descpb.ID(d.raw[i].ID), desc); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}

func MakeDescriptorTable(ctx context.Context, rows []DescriptorTableRow) (DescriptorTable, error) {
	dg, err := newDescGetter(ctx, rows)
	if err != nil {
		return DescriptorTable{}, err
	}
	ids := make([]descpb.ID, len(rows))
	for i := 0; i < len(rows); i++ {
		ids[i] = descpb.ID(rows[i].ID)
	}
	unwrapped, err := dg.GetDescs(ctx, ids)
	if err != nil {
		return DescriptorTable{}, err
	}
	return DescriptorTable{
		raw:        rows,
		unwrapped:  unwrapped,
		DescGetter: dg,
	}, nil
}

// NamespaceTableRow represents a namespace entry from table system.namespace.
type NamespaceTableRow struct {
	descpb.NameInfo
	ID int64
}

// NamespaceTable represents data read from `system.namespace2`.
type NamespaceTable struct {
	raw []NamespaceTableRow
	m   map[descpb.NameInfo]descpb.ID
}

func (n *NamespaceTable) GetNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, bool, error) {
	id, ok := n.m[descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}]
	return id, ok, nil
}

func (n *NamespaceTable) NumEntries() int {
	return len(n.m)
}

// MakeNamespaceTable constructs a new NamespaceTable.
func MakeNamespaceTable(rows []NamespaceTableRow) NamespaceTable {
	m := make(map[descpb.NameInfo]descpb.ID, len(rows))
	for _, r := range rows {
		m[r.NameInfo] = descpb.ID(r.ID)
	}
	return NamespaceTable{
		raw: rows,
		m:   m,
	}
}

var _ catalog.NamespaceGetter = (*NamespaceTable)(nil)

// namespaceReverseMap is the inverse of the namespace map stored in table
// `system.namespace`.
type namespaceReverseMap map[descpb.ID][]descpb.NameInfo

// JobsTable represents data read from `system.jobs`.
type JobsTable []jobs.JobMetadata

func newDescGetter(ctx context.Context, rows []DescriptorTableRow) (catalog.MapDescGetter, error) {
	pg := catalog.MapDescGetter{}
	for _, r := range rows {
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(r.DescBytes, &d); err != nil {
			return nil, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
		}
		descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, &d, r.ModTime)
		pg[descpb.ID(r.ID)] = catalogkv.UnwrapDescriptorRaw(ctx, &d)
	}
	return pg, nil
}

func newNamespaceMap(rows []NamespaceTableRow) namespaceReverseMap {
	res := make(namespaceReverseMap)
	for _, r := range rows {
		id := descpb.ID(r.ID)
		l, ok := res[id]
		if !ok {
			res[id] = []descpb.NameInfo{r.NameInfo}
		} else {
			res[id] = append(l, r.NameInfo)
		}
	}
	return res
}

// Examine runs a suite of consistency checks over system tables.
func Examine(
	ctx context.Context,
	descTable DescriptorSet,
	namespaceTable *NamespaceTable,
	jobsTable JobsTable,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	report := func(id descpb.ID, desc catalog.Descriptor, err error) {
		io.WriteString(stdout, formatDescriptorMsg(id, desc, "%v\n", err))
	}
	descOk, err := examineDescriptors(ctx, descTable, namespaceTable, report)
	if err != nil {
		return false, err
	}
	jobsOk, err := examineJobs(ctx, descTable, jobsTable, verbose, stdout)
	if err != nil {
		return false, err
	}
	return descOk && jobsOk, nil
}

type recordingNamespaceGetter struct {
	seen map[descpb.NameInfo]struct{}
	catalog.NamespaceGetter
}

func (r recordingNamespaceGetter) GetNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, bool, error) {
	r.seen[descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}] = struct{}{}
	return r.NamespaceGetter.GetNamespaceEntry(ctx, parentID, parentSchemaID, name)
}

// examineDescriptors runs a suite of checks over the descriptor table.
func examineDescriptors(
	ctx context.Context,
	descTable DescriptorSet,
	namespaceTable *NamespaceTable,
	reportFunc func(descpb.ID, catalog.Descriptor, error),
) (ok bool, err error) {
	log.Infof(ctx,
		"examining %d descriptors and %d namespace entries...\n",
		descTable.NumDescriptors(), namespaceTable.NumEntries())
	const (
		namespaceTableID  = 2
		namespace2TableID = 30
	)
	var problemsFound bool
	report := func(id descpb.ID, descriptor catalog.Descriptor, err error) {
		problemsFound = true
		reportFunc(id, descriptor, err)
	}
	reportf := func(
		id descpb.ID, descriptor catalog.Descriptor, msg string, args ...interface{},
	) {
		report(id, descriptor, errors.NewWithDepthf(1, msg, args...))
	}

	ns := recordingNamespaceGetter{
		seen:            make(map[descpb.NameInfo]struct{}),
		NamespaceGetter: namespaceTable,
	}
	descTable.Iterate(func(id descpb.ID, desc catalog.Descriptor) error {
		if desc.GetID() != id {
			reportf(id, desc,
				"different id in descriptor table: %d", id)
		}

		switch d := desc.(type) {
		case catalog.TableDescriptor:
			if err := d.Validate(ctx, descTable, ns); err != nil {
				report(desc.GetID(), desc, err)
			}
		case catalog.TypeDescriptor:
			typ := typedesc.NewImmutable(*d.TypeDesc())
			if err := typ.Validate(ctx, descTable); err != nil {
				report(desc.GetID(), desc, err)
			}
		case catalog.SchemaDescriptor:
			// TODO(ajwerner): Validate this.
		case catalog.DatabaseDescriptor:
			// TODO(ajwerner): Validate this.
		}
		log.VEventf(ctx, 2, "processed descriptor %d: %v", desc.GetID(), desc)
		return nil
	})

	// Now go over all namespace entries that don't point to descriptors in the
	// descriptor table.
	for _, row := range namespaceTable.raw {
		id := descpb.ID(row.ID)
		if row.ID == keys.PublicSchemaID {
			continue
		}
		if id == descpb.InvalidID {
			reportf(id, nil, "Row(s) %+v: NULL value found\n", row.NameInfo)
			continue
		}
		if strings.HasPrefix(row.NameInfo.Name, "pg_temp_") {
			// Temporary schemas have namespace entries but not descriptors.
			continue
		}
		if _, ok := ns.seen[row.NameInfo]; ok {
			continue
		}
		reportf(id, nil,
			"has namespace row(s) %+v but no descriptor\n", row.NameInfo)
	}
	return !problemsFound, err
}

// examineJobs runs a suite of consistency checks over the system.jobs table.
func examineJobs(
	ctx context.Context, descTable DescriptorSet, jobsTable JobsTable, verbose bool, stdout io.Writer,
) (ok bool, err error) {
	fmt.Fprintf(stdout, "Examining %d running jobs...\n", len(jobsTable))
	if err != nil {
		return false, err
	}
	problemsFound := false
	for _, j := range jobsTable {
		if verbose {
			fmt.Fprintf(stdout, "Processing job %d\n", j.ID)
		}
		if j.Payload.Type() != jobspb.TypeSchemaChangeGC {
			continue
		}
		existingTables := make([]int64, 0)
		missingTables := make([]int64, 0)
		for _, table := range j.Progress.GetSchemaChangeGC().Tables {
			if table.Status == jobspb.SchemaChangeGCProgress_DELETED {
				continue
			}
			tableDesc, err := descTable.GetDesc(ctx, table.ID)
			if err != nil {
				// TODO(ajwerner): Report this rather than returning it. Some errors
				// probably should lead to early termination but separating those out
				// is hard. Maybe all errors here indicate a real problem.
				return false, err
			}
			if tableExists := tableDesc != nil; tableExists {
				existingTables = append(existingTables, int64(table.ID))
			} else {
				missingTables = append(missingTables, int64(table.ID))
			}
		}

		if len(missingTables) > 0 {
			problemsFound = true
			sortkeys.Int64s(missingTables)
			fmt.Fprintf(stdout, "job %d: schema change GC refers to missing table descriptor(s) %+v\n"+
				"\texisting descriptors that still need to be dropped %+v\n",
				j.ID, missingTables, existingTables)
			if len(existingTables) == 0 && len(j.Progress.GetSchemaChangeGC().Indexes) == 0 {
				fmt.Fprintf(stdout, "\tjob %d can be safely deleted\n", j.ID)
			}
		}
	}
	return !problemsFound, nil
}

func formatDescriptorMsg(
	id descpb.ID, desc catalog.Descriptor, format string, args ...interface{},
) string {
	var header string
	switch desc.(type) {
	case catalog.TypeDescriptor:
		header = "    Type"
	case catalog.TableDescriptor:
		header = "   Table"
	case catalog.SchemaDescriptor:
		header = "  Schema"
	case catalog.DatabaseDescriptor:
		header = "Database"
	case nil:
		return fmt.Sprintf("     nil %3d: %s", id, fmt.Sprintf(format, args...))
	}
	return fmt.Sprintf("%s %3d: ParentID %3d, ParentSchemaID %2d, Name '%s': ",
		header, desc.GetID(), desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()) +
		fmt.Sprintf(format, args...)
}
