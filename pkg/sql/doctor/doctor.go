// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package doctor provides utilities for checking the consistency of cockroach
// internal persisted metadata.
package doctor

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
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

var _ catalog.NameEntry = (*NamespaceTableRow)(nil)

// GetID implements the catalog.NameEntry interface.
func (nsr *NamespaceTableRow) GetID() descpb.ID {
	return descpb.ID(nsr.ID)
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

func processDescriptorTable(
	stdout io.Writer, descRows []DescriptorTableRow,
) (func(descpb.ID) catalog.Descriptor, error) {
	m := make(map[int64]catalog.Descriptor, len(descRows))
	// Build the map first with un-upgraded descriptors.
	for _, r := range descRows {
		b, err := descbuilder.FromBytesAndMVCCTimestamp(r.DescBytes, r.ModTime)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal descriptor %d", r.ID)
		}
		if b != nil {
			if err := b.RunPostDeserializationChanges(); err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err, "error during RunPostDeserializationChanges")
			}
			m[r.ID] = b.BuildImmutable()
		}
	}
	// Run post-restore upgrades.
	for _, r := range descRows {
		desc := m[r.ID]
		if desc == nil {
			continue
		}
		b := desc.NewBuilder()
		if err := b.RunPostDeserializationChanges(); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err, "error during RunPostDeserializationChanges")
		}
		if err := b.RunRestoreChanges(clusterversion.TestingClusterVersion, func(id descpb.ID) catalog.Descriptor {
			return m[int64(id)]
		}); err != nil {
			descReport(stdout, desc, "failed to upgrade descriptor: %v", err)
			continue
		}
		m[r.ID] = b.BuildImmutable()
	}
	return func(id descpb.ID) catalog.Descriptor {
		return m[int64(id)]
	}, nil
}

// Examine runs a suite of consistency checks over system tables.
func Examine(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	descTable DescriptorTable,
	namespaceTable NamespaceTable,
	jobsTable JobsTable,
	validiateJobs bool,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	descOk, err := ExamineDescriptors(
		ctx,
		version,
		descTable,
		namespaceTable,
		jobsTable,
		validiateJobs,
		verbose,
		stdout)
	if err != nil {
		return false, err
	}
	jobsOk := true
	if validiateJobs {
		jobsOk, err = ExamineJobs(ctx, descTable, jobsTable, verbose, stdout)
		if err != nil {
			return false, err
		}
	}
	return descOk && jobsOk, nil
}

// ExamineDescriptors runs a suite of checks over the descriptor table.
func ExamineDescriptors(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	descTable DescriptorTable,
	namespaceTable NamespaceTable,
	jobsTable JobsTable,
	validateJobs bool,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	fmt.Fprintf(
		stdout, "Examining %d descriptors and %d namespace entries...\n",
		len(descTable), len(namespaceTable))
	descLookupFn, err := processDescriptorTable(stdout, descTable)
	if err != nil {
		return false, err
	}
	var problemsFound bool
	var cb nstree.MutableCatalog
	for _, row := range namespaceTable {
		cb.UpsertNamespaceEntry(row.NameInfo, descpb.ID(row.ID), hlc.Timestamp{})
	}
	for _, row := range descTable {
		id := descpb.ID(row.ID)
		desc := descLookupFn(id)
		if desc == nil {
			// This should never happen as ids are parsed and inserted from descTable.
			log.Fatalf(ctx, "Descriptor ID %d not found", row.ID)
		}
		if desc.GetID() != id {
			problemsFound = true
			descReport(stdout, desc, "different id in descriptor table: %d", row.ID)
			continue
		}
		cb.UpsertDescriptor(desc)
	}
	for _, row := range descTable {
		id := descpb.ID(row.ID)
		desc := descLookupFn(id)
		// No need to validate dropped descriptors
		if desc.Dropped() {
			continue
		}
		ve := cb.ValidateWithRecover(ctx, version, desc)
		for _, err := range ve {
			problemsFound = true
			descReport(stdout, desc, "%s", err)
		}

		if validateJobs {
			jobs.ValidateJobReferencesInDescriptor(desc, jobsTable, func(err error) {
				problemsFound = true
				descReport(stdout, desc, "%s", err)
			})
		}

		if verbose {
			descReport(stdout, desc, "processed")
		}
	}
	for _, row := range namespaceTable {
		err := cb.ValidateNamespaceEntry(row)
		if err != nil {
			problemsFound = true
			nsReport(stdout, row, "%s", err)
		} else if verbose {
			nsReport(stdout, row, "processed")
		}
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
	fmt.Fprintf(stdout, "Examining %d jobs...\n", len(jobsTable))
	descLookupFn, err := processDescriptorTable(stdout, descTable)
	if err != nil {
		return false, err
	}
	problemsFound := false
	for _, j := range jobsTable {
		if verbose {
			fmt.Fprintf(stdout, "Processing job %d\n", j.ID)
		}
		jobs.ValidateDescriptorReferencesInJob(j, descLookupFn, func(err error) {
			problemsFound = true
			fmt.Fprintf(stdout, "job %d: %s.\n", j.ID, err)
		}, func(s string) {
			fmt.Fprintf(stdout, "job %d: %s.\n", j.ID, s)
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
	// Assume the target is an empty cluster with the same binary version
	ms := bootstrap.MakeMetadataSchema(keys.SystemSQLCodec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
	minUserDescID := ms.FirstNonSystemDescriptorID()
	minUserCreatedDescID := minUserDescID + descpb.ID(len(catalogkeys.DefaultUserDBs))*2
	// Print first transaction, which removes all predefined user descriptors.
	fmt.Fprintln(out, `BEGIN;`)
	// Add a query which triggers a divide-by-zero error when the txn runs on a
	// non-empty cluster (excluding predefined user descriptors).
	fmt.Fprintf(out,
		"SELECT 1/(1-sign(count(*))) FROM system.descriptor WHERE id >= %d;\n",
		minUserCreatedDescID)
	// Delete predefined user descriptors.
	fmt.Fprintf(out,
		"SELECT crdb_internal.unsafe_delete_descriptor(id, true) FROM system.descriptor WHERE id >= %d;\n",
		minUserDescID)
	// Delete predefined user descriptor namespace entries.
	fmt.Fprintf(out,
		"SELECT crdb_internal.unsafe_delete_namespace_entry(\"parentID\", \"parentSchemaID\", name, id) "+
			"FROM system.namespace WHERE id >= %d;\n",
		minUserDescID)
	fmt.Fprintln(out, `COMMIT;`)
	// Print second transaction, which inserts namespace and descriptor entries.
	fmt.Fprintln(out, `BEGIN;`)
	reverseNamespace := make(map[int64][]NamespaceTableRow, len(descTable))
	for _, row := range namespaceTable {
		reverseNamespace[row.ID] = append(reverseNamespace[row.ID], row)
	}
	for _, descRow := range descTable {
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
		if namespaceRow.ID == keys.SystemDatabaseID || namespaceRow.ParentID == keys.SystemDatabaseID {
			// Skip system entries.
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

// descriptorModifiedForInsert updates the descriptor representation to make it
// safe to insert:
// - set the version to 1,
// - unset the descriptor modification time,
// - unset the descriptor create-as-of time, for table descriptors.
// Also, skip system descriptors.
func descriptorModifiedForInsert(r DescriptorTableRow) ([]byte, error) {
	var descProto descpb.Descriptor
	if err := protoutil.Unmarshal(r.DescBytes, &descProto); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal descriptor %d", r.ID)
	}
	switch u := descProto.Union.(type) {
	case *descpb.Descriptor_Table:
		if u.Table.ParentID == keys.SystemDatabaseID {
			return nil, nil
		}
		u.Table.CreateAsOfTime.Reset()
		u.Table.ModificationTime.Reset()
		u.Table.Version = 1
	case *descpb.Descriptor_Database:
		if u.Database.ID == keys.SystemDatabaseID {
			return nil, nil
		}
		u.Database.ModificationTime.Reset()
		u.Database.Version = 1
	case *descpb.Descriptor_Schema:
		if u.Schema.ParentID == keys.SystemDatabaseID {
			return nil, nil
		}
		u.Schema.ModificationTime.Reset()
		u.Schema.Version = 1
	case *descpb.Descriptor_Type:
		if u.Type.ParentID == keys.SystemDatabaseID {
			return nil, nil
		}
		u.Type.ModificationTime.Reset()
		u.Type.Version = 1
	case *descpb.Descriptor_Function:
		if u.Function.ParentID == keys.SystemDatabaseID {
			return nil, nil
		}
		u.Function.ModificationTime.Reset()
		u.Function.Version = 1
	default:
		return nil, nil
	}
	return protoutil.Marshal(&descProto)
}
