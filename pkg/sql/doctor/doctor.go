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

// DescriptorTable represents data read from `system.descriptor`.
type DescriptorTable []DescriptorTableRow

// NamespaceTableRow represents a namespace entry from table system.namespace.
type NamespaceTableRow struct {
	descpb.NameInfo
	ID int64
}

// NamespaceTable represents data read from `system.namespace2`.
type NamespaceTable []NamespaceTableRow

// namespaceReverseMap is the inverse of the namespace map stored in table
// `system.namespace`.
type namespaceReverseMap map[int64][]descpb.NameInfo

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
		l, ok := res[r.ID]
		if !ok {
			res[r.ID] = []descpb.NameInfo{r.NameInfo}
		} else {
			res[r.ID] = append(l, r.NameInfo)
		}
	}
	return res
}

// Examine runs a suite of consistency checks over system tables.
func Examine(
	ctx context.Context,
	descTable DescriptorTable,
	namespaceTable NamespaceTable,
	jobsTable JobsTable,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	descOk, err := ExamineDescriptors(ctx, descTable, namespaceTable, verbose, stdout)
	if err != nil {
		return false, err
	}
	jobsOk, err := ExamineJobs(ctx, descTable, jobsTable, verbose, stdout)
	if err != nil {
		return false, err
	}
	return descOk && jobsOk, nil
}

// ExamineDescriptors runs a suite of checks over the descriptor table.
func ExamineDescriptors(
	ctx context.Context,
	descTable DescriptorTable,
	namespaceTable NamespaceTable,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	const (
		namespaceTableID  = 2
		namespace2TableID = 30
	)

	fmt.Fprintf(
		stdout, "Examining %d descriptors and %d namespace entries...\n",
		len(descTable), len(namespaceTable))
	descGetter, err := newDescGetter(ctx, descTable)
	if err != nil {
		return false, err
	}
	nMap := newNamespaceMap(namespaceTable)

	var problemsFound bool
	for _, row := range descTable {
		desc, ok := descGetter[descpb.ID(row.ID)]
		if !ok {
			// This should never happen as ids are parsed and inserted from descTable.
			log.Fatalf(ctx, "Descriptor id %d not found", row.ID)
		}

		if int64(desc.GetID()) != row.ID {
			fmt.Fprint(stdout, reportMsg(desc, "different id in descriptor table: %d", row.ID))
			problemsFound = true
			continue
		}

		_, parentExists := descGetter[desc.GetParentID()]
		parentSchema, parentSchemaExists := descGetter[desc.GetParentSchemaID()]
		switch d := desc.(type) {
		case catalog.TableDescriptor:
			if err := d.Validate(ctx, descGetter); err != nil {
				problemsFound = true
				fmt.Fprint(stdout, reportMsg(desc, "%s", err))
			}
			// Table has been already validated.
			parentExists = true
			parentSchemaExists = true
		case catalog.TypeDescriptor:
			typ := typedesc.NewImmutable(*d.TypeDesc())
			if err := typ.Validate(ctx, descGetter); err != nil {
				problemsFound = true
				fmt.Fprint(stdout, reportMsg(desc, "%s", err))
			}
		case catalog.SchemaDescriptor:
			// parent schema id is always 0.
			parentSchemaExists = true
		}
		var invalidParentID bool
		if desc.GetParentID() != descpb.InvalidID && !parentExists {
			problemsFound = true
			invalidParentID = true
			fmt.Fprint(stdout, reportMsg(desc, "invalid parent id %d", desc.GetParentID()))
		}
		if desc.GetParentSchemaID() != descpb.InvalidID &&
			desc.GetParentSchemaID() != keys.PublicSchemaID {
			if !parentSchemaExists {
				problemsFound = true
				fmt.Fprint(stdout, reportMsg(desc, "invalid parent schema id %d", desc.GetParentSchemaID()))
			} else if !invalidParentID && parentSchema.GetParentID() != desc.GetParentID() {
				problemsFound = true
				fmt.Fprint(stdout, reportMsg(desc, "invalid parent id of parent schema, expected %d, found %d", desc.GetParentID(), parentSchema.GetParentID()))
			}
		}

		// Process namespace entries pointing to this descriptor.
		names, ok := nMap[row.ID]
		if !ok {
			// TODO(spaskob): this check is too crude, we need more fine grained
			// approach depending on all the possible non-20.1 possibilities and emit
			// a warning if one of those states is encountered without returning a
			// nonzero exit status and fail otherwise.
			// See https://github.com/cockroachdb/cockroach/issues/55237.
			if !desc.Dropped() && desc.GetID() != namespaceTableID {
				fmt.Fprint(stdout, reportMsg(desc, "not being dropped but no namespace entry found"))
				problemsFound = true
			}
			continue
		}

		if desc.Dropped() {
			fmt.Fprint(stdout, reportMsg(desc, "dropped but namespace entry(s) found: %v", names))
			problemsFound = true
		}

		// We delete all pointed descriptors to leave what is missing in the
		// descriptor table.
		delete(nMap, row.ID)

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
				fmt.Fprint(
					stdout,
					reportMsg(desc, "namespace entry %+v not found in draining names", n),
				)
				problemsFound = true
			}
		}
		if !found && desc.GetID() != namespace2TableID {
			fmt.Fprint(stdout, reportMsg(desc, "could not find name in namespace table"))
			problemsFound = true
			continue
		}
		if len(drainingNames) > 0 {
			fmt.Fprint(stdout, reportMsg(desc, "extra draining names found %+v", drainingNames))
			problemsFound = true
		}
		if verbose {
			fmt.Fprint(stdout, reportMsg(desc, "processed"))
		}
	}

	// Now go over all namespace entries that don't point to descriptors in the
	// descriptor table.
	for id, ni := range nMap {
		if id == keys.PublicSchemaID {
			continue
		}
		if descpb.ID(id) == descpb.InvalidID {
			fmt.Fprintf(stdout, "Row(s) %+v: NULL value found\n", ni)
			problemsFound = true
			continue
		}
		if strings.HasPrefix(ni[0].Name, "pg_temp_") {
			// Temporary schemas have namespace entries but not descriptors.
			continue
		}
		fmt.Fprintf(
			stdout, "Descriptor %d: has namespace row(s) %+v but no descriptor\n", id, ni)
		problemsFound = true
	}
	return !problemsFound, err
}

// ExamineJobs runs a suite of consistency checks over the system.jobs table.
func ExamineJobs(
	ctx context.Context,
	descTable DescriptorTable,
	jobsTable JobsTable,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	fmt.Fprintf(stdout, "Examining %d running jobs...\n", len(jobsTable))
	descGetter, err := newDescGetter(ctx, descTable)
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
			_, tableExists := descGetter[table.ID]
			if tableExists {
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

func reportMsg(desc catalog.Descriptor, format string, args ...interface{}) string {
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
	}
	return fmt.Sprintf("%s %3d: ParentID %3d, ParentSchemaID %2d, Name '%s': ",
		header, desc.GetID(), desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()) +
		fmt.Sprintf(format, args...) + "\n"
}
