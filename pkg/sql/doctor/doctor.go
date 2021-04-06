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

// JobsTable represents data read from `system.jobs`.
type JobsTable []jobs.JobMetadata

func newDescGetter(
	ctx context.Context, stdout io.Writer, descRows []DescriptorTableRow, nsRows []NamespaceTableRow,
) (catalog.MapDescGetter, error) {
	ddg := catalog.MapDescGetter{
		Descriptors: make(map[descpb.ID]catalog.Descriptor, len(descRows)),
		Namespace:   make(map[descpb.NameInfo]descpb.ID, len(nsRows)),
	}
	// Build the descGetter first with un-upgraded descriptors.
	for _, r := range descRows {
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(r.DescBytes, &d); err != nil {
			return ddg, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
		}
		b := catalogkv.NewBuilderWithMVCCTimestamp(&d, r.ModTime)
		if b != nil {
			ddg.Descriptors[descpb.ID(r.ID)] = b.BuildImmutable()
		}
	}
	// Rebuild the descGetter with upgrades.
	for _, r := range descRows {
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(r.DescBytes, &d); err != nil {
			return ddg, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
		}
		b := catalogkv.NewBuilderWithMVCCTimestamp(&d, r.ModTime)
		if b != nil {
			if err := b.RunPostDeserializationChanges(ctx, ddg); err != nil {
				descReport(stdout, ddg.Descriptors[descpb.ID(r.ID)], "failed to upgrade descriptor: %v", err)
			} else {
				ddg.Descriptors[descpb.ID(r.ID)] = b.BuildImmutable()
			}
		}
	}
	for _, r := range nsRows {
		ddg.Namespace[r.NameInfo] = descpb.ID(r.ID)
	}
	return ddg, nil
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
	fmt.Fprintf(
		stdout, "Examining %d descriptors and %d namespace entries...\n",
		len(descTable), len(namespaceTable))
	ddg, err := newDescGetter(ctx, stdout, descTable, namespaceTable)
	if err != nil {
		return false, err
	}
	var problemsFound bool

	for _, row := range descTable {
		desc, ok := ddg.Descriptors[descpb.ID(row.ID)]
		if !ok {
			// This should never happen as ids are parsed and inserted from descTable.
			log.Fatalf(ctx, "Descriptor id %d not found", row.ID)
		}

		if int64(desc.GetID()) != row.ID {
			descReport(stdout, desc, "different id in descriptor table: %d", row.ID)
			problemsFound = true
			continue
		}
		for _, err := range validateSafely(ctx, ddg, desc) {
			problemsFound = true
			descReport(stdout, desc, "%s", err)
		}
		if verbose {
			descReport(stdout, desc, "processed")
		}
	}

	for _, row := range namespaceTable {
		desc := ddg.Descriptors[descpb.ID(row.ID)]
		err := validateNamespaceRow(row, desc)
		if err != nil {
			nsReport(stdout, row, err.Error())
			problemsFound = true
		} else if verbose {
			nsReport(stdout, row, "processed")
		}
	}

	return !problemsFound, err
}

func validateSafely(
	ctx context.Context, descGetter catalog.DescGetter, desc catalog.Descriptor,
) (errs []error) {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = errors.Newf("%v", r)
			}
			err = errors.WithAssertionFailure(errors.Wrap(err, "validation"))
			errs = append(errs, err)
		}
	}()
	results := catalog.Validate(ctx, descGetter, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, desc)
	errs = append(errs, results.Errors()...)
	return errs
}

func validateNamespaceRow(row NamespaceTableRow, desc catalog.Descriptor) error {
	id := descpb.ID(row.ID)
	if id == keys.PublicSchemaID {
		// The public schema doesn't have a descriptor.
		return nil
	}
	isSchema := row.ParentID != keys.RootNamespaceID && row.ParentSchemaID == keys.RootNamespaceID
	if isSchema && strings.HasPrefix(row.Name, "pg_temp_") {
		// Temporary schemas have namespace entries but not descriptors.
		return nil
	}
	if id == descpb.InvalidID {
		return fmt.Errorf("invalid descriptor ID")
	}
	if desc == nil {
		return catalog.ErrDescriptorNotFound
	}
	for _, dn := range desc.GetDrainingNames() {
		if dn == row.NameInfo {
			return nil
		}
	}
	if desc.Dropped() {
		return fmt.Errorf("no matching name info in draining names of dropped %s",
			string(desc.DescriptorType()))
	}
	if row.ParentID == desc.GetParentID() &&
		row.ParentSchemaID == desc.GetParentSchemaID() &&
		row.Name == desc.GetName() {
		return nil
	}
	return fmt.Errorf("no matching name info found in non-dropped %s %q",
		string(desc.DescriptorType()), desc.GetName())
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
	ddg, err := newDescGetter(ctx, stdout, descTable, nil)
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
			_, tableExists := ddg.Descriptors[table.ID]
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

func nsReport(stdout io.Writer, row NamespaceTableRow, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintf(stdout, "  ParentID %3d, ParentSchemaID %2d: namespace entry %q (%d): %s\n",
		row.ParentID, row.ParentSchemaID, row.Name, row.ID, msg)
}

func descReport(stdout io.Writer, desc catalog.Descriptor, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	// Add descriptor-identifying prefix if it isn't there already.
	// The prefix has the same format as the validation error wrapper.
	msgPrefix := fmt.Sprintf("%s %q (%d): ", desc.DescriptorType(), desc.GetName(), desc.GetID())
	if msg[:len(msgPrefix)] == msgPrefix {
		msgPrefix = ""
	}
	_, _ = fmt.Fprintf(stdout, "  ParentID %3d, ParentSchemaID %2d: %s%s\n",
		desc.GetParentID(), desc.GetParentSchemaID(), msgPrefix, msg)
}
