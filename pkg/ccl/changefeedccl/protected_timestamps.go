// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// createCombinedProtectedTimestampRecord will create a record to
// protect the spans for this changefeed at the resolved timestamp. It will
// protect both the system tables and the user tables. This is the legacy style
// protected timestamp record that was used before per-table protected timestamps
// were introduced where we protect system tables in their own record.
func createCombinedProtectedTimestampRecord(
	ctx context.Context, jobID jobspb.JobID, targets changefeedbase.Targets, resolved hlc.Timestamp,
) *ptpb.Record {
	ptsID := uuid.MakeV4()
	targetToProtect := makeCombinedTargetToProtect(targets)

	log.VEventf(ctx, 2, "creating protected timestamp %v at %v", ptsID, resolved)
	return jobsprotectedts.MakeRecord(
		ptsID, int64(jobID), resolved, jobsprotectedts.Jobs, targetToProtect)
}

// createSystemTablesProtectedTimestampRecord will create a record to
// protect the system tables at the resolved timestamp. We use this to protect
// system tables separately from the user tables when per-table protected
// timestamps are enabled to avoid duplicating the system tables protections
// in each of the user tables' protected timestamp records.
func createSystemTablesProtectedTimestampRecord(
	ctx context.Context, jobID jobspb.JobID, resolved hlc.Timestamp,
) *ptpb.Record {
	ptsID := uuid.MakeV4()

	log.VEventf(ctx, 2, "creating protected timestamp for system tables %v at %v", ptsID, resolved)
	return jobsprotectedts.MakeRecord(
		ptsID, int64(jobID), resolved, jobsprotectedts.Jobs, makeSystemTablesTargetToProtect(),
	)
}

// createUserTablesProtectedTimestampRecord will create a record to protect the
// user tables at the resolved timestamp. We use this to protect user tables
// separately from the system tables when per-table protected timestamps are
// enabled.
func createUserTablesProtectedTimestampRecord(
	ctx context.Context, jobID jobspb.JobID, targets changefeedbase.Targets, resolved hlc.Timestamp,
) *ptpb.Record {
	ptsID := uuid.MakeV4()

	log.VEventf(ctx, 2, "creating protected timestamp for user tables %v at %v", ptsID, resolved)
	return jobsprotectedts.MakeRecord(
		ptsID, int64(jobID), resolved, jobsprotectedts.Jobs,
		makeUserTablesTargetToProtect(targets),
	)
}

// systemTablesToProtect holds the descriptor IDs of the system tables
// that need to be protected to ensure that a CHANGEFEED can do a
// historical read of a table descriptor.
var systemTablesToProtect = []descpb.ID{
	keys.DescriptorTableID,
	keys.UsersTableID,
	keys.ZonesTableID,
	keys.RoleMembersTableID,
	keys.CommentsTableID,
	keys.NamespaceTableID,
	keys.RoleOptionsTableID,
	// These can be identified by the TestChangefeedIdentifyDependentTablesForProtecting test.
}

// makeCombinedTargetToProtect will create a target to protect the spans for the given
// targets. It adds the system tables to the specified targets. Note that this
// is only used for the legacy style (non-per-table) protected timestamp record.
func makeCombinedTargetToProtect(targets changefeedbase.Targets) *ptpb.Target {
	tablesToProtect := make(descpb.IDs, 0, targets.NumUniqueTables()+len(systemTablesToProtect))
	_ = targets.EachTableID(func(id descpb.ID) error {
		tablesToProtect = append(tablesToProtect, id)
		return nil
	})
	tablesToProtect = append(tablesToProtect, systemTablesToProtect...)
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

// makeUserTablesTargetToProtect will create a target to protect the spans for
// the given targets. It does not add the system tables to the specified targets.
// This is used when per-table protected timestamps are enabled to protect only
// the user tables.
func makeUserTablesTargetToProtect(targets changefeedbase.Targets) *ptpb.Target {
	tablesToProtect := make(descpb.IDs, 0, targets.NumUniqueTables())
	_ = targets.EachTableID(func(id descpb.ID) error {
		tablesToProtect = append(tablesToProtect, id)
		return nil
	})
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

// makeSystemTablesTargetToProtect will create a target to protect the spans for
// the system tables. It is used when per-table protected timestamps are enabled.
// In that case we protect the system tables separately from the user tables.
func makeSystemTablesTargetToProtect() *ptpb.Target {
	return ptpb.MakeSchemaObjectsTarget(systemTablesToProtect)
}
