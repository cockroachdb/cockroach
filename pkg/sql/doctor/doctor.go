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
type NamespaceTable []NamespaceTableRow

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
	namespaceTable NamespaceTable,
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

// examineDescriptors runs a suite of checks over the descriptor table.
func examineDescriptors(
	ctx context.Context,
	descTable DescriptorSet,
	namespaceTable NamespaceTable,
	reportFunc func(descpb.ID, catalog.Descriptor, error),
) (ok bool, err error) {
	log.Infof(ctx,
		"examining %d descriptors and %d namespace entries...\n",
		descTable.NumDescriptors(), len(namespaceTable))
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
	nMap := newNamespaceMap(namespaceTable)
	descTable.Iterate(func(id descpb.ID, desc catalog.Descriptor) error {
		if desc.GetID() != id {
			reportf(id, desc,
				"different id in descriptor table: %d", id)
		}

		parent, err := descTable.GetDesc(ctx, desc.GetParentID())
		if err != nil {
			report(id, desc, errors.Wrapf(err, "failed to get parent %d", desc.GetParentID()))
		}
		parentExists := parent != nil
		parentSchema, err := descTable.GetDesc(ctx, desc.GetParentID())
		if err != nil {
			report(id, desc, errors.Wrapf(err, "failed to get parent schema %d", desc.GetParentSchemaID()))
		}
		parentSchemaExists := parentSchema != nil
		switch d := desc.(type) {
		case catalog.TableDescriptor:
			if err := d.Validate(ctx, descTable); err != nil {
				report(desc.GetID(), desc, err)
			}
			// Table has been already validated.
			parentExists = true
			parentSchemaExists = true
		case catalog.TypeDescriptor:
			typ := typedesc.NewImmutable(*d.TypeDesc())
			if err := typ.Validate(ctx, descTable); err != nil {
				report(desc.GetID(), desc, err)
			}
		case catalog.SchemaDescriptor:
			// parent schema id is always 0.
			parentSchemaExists = true
		}
		if desc.GetParentID() != descpb.InvalidID && !parentExists {
			reportf(desc.GetID(), desc, "invalid parent id %d", desc.GetParentID())
		}
		if desc.GetParentSchemaID() != descpb.InvalidID &&
			desc.GetParentSchemaID() != keys.PublicSchemaID &&
			!parentSchemaExists {
			reportf(desc.GetID(), desc, "invalid parent schema id %d", desc.GetParentSchemaID())
		}

		// Process namespace entries pointing to this descriptor.
		names, ok := nMap[id]
		if !ok {
			// TODO(spaskob): this check is too crude, we need more fine grained
			// approach depending on all the possible non-20.1 possibilities and emit
			// a warning if one of those states is encountered without returning a
			// nonzero exit status and fail otherwise.
			// See https://github.com/cockroachdb/cockroach/issues/55237.
			if !desc.Dropped() && desc.GetID() != namespaceTableID {
				reportf(desc.GetID(), desc, "not being dropped but no namespace entry found")
			}
			return nil
		}

		if desc.Dropped() {
			reportf(desc.GetID(), desc, "dropped but namespace entry(s) found: %v", names)
		}

		// We delete all pointed descriptors to leave what is missing in the
		// descriptor table.
		delete(nMap, id)

		drainingNames := desc.GetDrainingNames()
		var found bool
		for _, n := range names {
			if n.Name == desc.GetName() &&
				n.ParentSchemaID == desc.GetParentSchemaID() &&
				n.ParentID == desc.GetParentID() {
				found = true
				continue
			}
			var foundInDraining bool
			for i, drain := range drainingNames {
				// If the namespace entry does not correspond to the current descriptor
				// name then it must be found in the descriptor draining names.
				if drain.Name == n.Name &&
					drain.ParentID == n.ParentID &&
					drain.ParentSchemaID == n.ParentSchemaID {
					// Delete this draining names entry from the list.
					last := len(drainingNames) - 1
					drainingNames[last], drainingNames[i] = drainingNames[i], drainingNames[last]
					drainingNames = drainingNames[:last]
					foundInDraining = true
					break
				}
			}
			if !foundInDraining && desc.GetID() != namespace2TableID {
				reportf(desc.GetID(), desc, "namespace entry %+v not found in draining names", n)
				problemsFound = true
			}
		}
		if !found && desc.GetID() != namespace2TableID {
			reportf(desc.GetID(), desc, "could not find name in namespace table")
			return nil
		}
		if len(drainingNames) > 0 {
			reportf(desc.GetID(), desc, "extra draining names found %+v", drainingNames)
		}
		log.VEventf(ctx, 2, "processed descriptor %d: %v", desc.GetID(), desc)
		return nil
	})

	// Now go over all namespace entries that don't point to descriptors in the
	// descriptor table.
	for id, ni := range nMap {
		if id == keys.PublicSchemaID {
			continue
		}
		if id == descpb.InvalidID {
			reportf(id, nil, "Row(s) %+v: NULL value found\n", ni)
			continue
		}
		if strings.HasPrefix(ni[0].Name, "pg_temp_") {
			// Temporary schemas have namespace entries but not descriptors.
			continue
		}
		reportf(id, nil,
			"has namespace row(s) %+v but no descriptor\n", ni)
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
