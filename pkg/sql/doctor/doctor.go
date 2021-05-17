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
	"encoding/hex"
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

// NamespaceTable represents data read from `system.namespace`.
type NamespaceTable []NamespaceTableRow

// JobsTable represents data read from `system.jobs`.
type JobsTable []jobs.JobMetadata

// GetJobMetadata implements the jobs.JobMetadataGetter interface.
func (jt JobsTable) GetJobMetadata(jobID jobspb.JobID) (*jobs.JobMetadata, error) {
	for i := range jt {
		md := &jt[i]
		if md.ID == jobID {
			return md, nil
		}
	}
	return nil, errors.Newf("job %d not found", jobID)
}

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
	descOk, err := ExamineDescriptors(ctx, descTable, namespaceTable, jobsTable, verbose, stdout)
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
	jobsTable JobsTable,
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
		ve := catalog.ValidateWithRecover(ctx, ddg, catalog.ValidationLevelAllPreTxnCommit, desc)
		for _, err := range ve.Errors() {
			problemsFound = true
			descReport(stdout, desc, "%s", err)
		}

		jobs.ValidateJobReferencesInDescriptor(desc, jobsTable, func(err error) {
			problemsFound = true
			descReport(stdout, desc, "%s", err)
		})

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
	fmt.Fprintf(stdout, "Examining %d jobs...\n", len(jobsTable))
	ddg, err := newDescGetter(ctx, stdout, descTable, nil)
	if err != nil {
		return false, err
	}
	problemsFound := false
	for _, j := range jobsTable {
		if verbose {
			fmt.Fprintf(stdout, "Processing job %d\n", j.ID)
		}
		jobs.ValidateDescriptorReferencesInJob(j, ddg.Descriptors, func(err error) {
			problemsFound = true
			fmt.Fprintf(stdout, "job %d: %s.\n", j.ID, err)
		})
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
	if strings.HasPrefix(msg, msgPrefix) {
		msgPrefix = ""
	}
	_, _ = fmt.Fprintf(stdout, "  ParentID %3d, ParentSchemaID %2d: %s%s\n",
		desc.GetParentID(), desc.GetParentSchemaID(), msgPrefix, msg)
}

// DumpSQL dumps SQL statements to an io.Writer to load the descriptor and
// namespace table contents into an empty cluster. System tables are not
// included. The descriptors themselves are as they were in the source cluster,
// with the possible exception of the version counter and the modification time
// timestamp.
func DumpSQL(out io.Writer, descTable DescriptorTable, namespaceTable NamespaceTable) error {
	// Print first transaction, which removes all predefined user descriptors.
	fmt.Fprintln(out, `BEGIN;`)
	// Add a query which triggers a divide-by-zero error when the txn runs on a
	// non-empty cluster (excluding predefined user descriptors).
	fmt.Fprintf(out,
		"SELECT 1/(1-sign(count(*))) FROM system.descriptor WHERE id >= %d;\n",
		keys.MinNonPredefinedUserDescID)
	// Delete predefined user descriptors.
	fmt.Fprintf(out,
		"SELECT crdb_internal.unsafe_delete_descriptor(id, true) FROM system.descriptor WHERE id >= %d;\n",
		keys.MinUserDescID)
	// Delete predefined user descriptor namespace entries.
	fmt.Fprintf(out,
		"SELECT crdb_internal.unsafe_delete_namespace_entry(\"parentID\", \"parentSchemaID\", name, id) "+
			"FROM system.namespace WHERE id >= %d;\n",
		keys.MinUserDescID)
	fmt.Fprintln(out, `COMMIT;`)
	// Print second transaction, which inserts namespace and descriptor entries.
	fmt.Fprintln(out, `BEGIN;`)
	reverseNamespace := make(map[int64][]NamespaceTableRow, len(descTable))
	for _, row := range namespaceTable {
		reverseNamespace[row.ID] = append(reverseNamespace[row.ID], row)
	}
	for _, descRow := range descTable {
		if descRow.ID < keys.MinUserDescID {
			// Skip system descriptors.
			continue
		}
		// Update the descriptor representation to make it safe to insert:
		// - set the version to 1,
		// - unset the descriptor modification time,
		// - unset the descriptor create-as-of time, for table descriptors.
		updatedDescBytes, err := descriptorModifiedForInsert(descRow)
		if err != nil {
			return err
		}
		if updatedDescBytes == nil {
			continue
		}
		fmt.Fprintf(out,
			"SELECT crdb_internal.unsafe_upsert_descriptor(%d, decode('%s', 'hex'), true);\n",
			descRow.ID, hex.EncodeToString(updatedDescBytes))
		for _, namespaceRow := range reverseNamespace[descRow.ID] {
			fmt.Fprintf(out,
				"SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, '%s', %d, true);\n",
				namespaceRow.ParentID, namespaceRow.ParentSchemaID, namespaceRow.Name, namespaceRow.ID)
		}
	}
	// Handle dangling namespace entries.
	for _, namespaceRow := range namespaceTable {
		if namespaceRow.ParentID == descpb.InvalidID && namespaceRow.ID < keys.MinUserDescID {
			// Skip system database entries.
			continue
		}
		if namespaceRow.ParentID != descpb.InvalidID && namespaceRow.ParentID < keys.MinUserDescID {
			// Skip non-database entries with system parent database.
			continue
		}
		if _, found := reverseNamespace[namespaceRow.ID]; found {
			// Skip entries for existing descriptors.
			continue
		}
		fmt.Fprintf(out,
			"SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, '%s', %d, true);\n",
			namespaceRow.ParentID, namespaceRow.ParentSchemaID, namespaceRow.Name, namespaceRow.ID)
	}
	fmt.Fprintln(out, `COMMIT;`)
	return nil
}

func descriptorModifiedForInsert(r DescriptorTableRow) ([]byte, error) {
	var descProto descpb.Descriptor
	if err := protoutil.Unmarshal(r.DescBytes, &descProto); err != nil {
		return nil, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
	}
	b := catalogkv.NewBuilderWithMVCCTimestamp(&descProto, r.ModTime)
	if b == nil {
		return nil, nil
	}
	mut := b.BuildCreatedMutable()
	switch d := mut.(type) {
	case catalog.DatabaseDescriptor:
		d.DatabaseDesc().ModificationTime = hlc.Timestamp{}
		d.DatabaseDesc().Version = 1
	case catalog.SchemaDescriptor:
		d.SchemaDesc().ModificationTime = hlc.Timestamp{}
		d.SchemaDesc().Version = 1
	case catalog.TypeDescriptor:
		d.TypeDesc().ModificationTime = hlc.Timestamp{}
		d.TypeDesc().Version = 1
	case catalog.TableDescriptor:
		d.TableDesc().ModificationTime = hlc.Timestamp{}
		d.TableDesc().CreateAsOfTime = hlc.Timestamp{}
		d.TableDesc().Version = 1
	default:
		return nil, nil
	}
	return protoutil.Marshal(mut.DescriptorProto())
}
