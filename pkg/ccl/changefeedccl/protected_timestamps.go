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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// createProtectedTimestampRecord will create a record to protect the spans for
// this changefeed at the resolved timestamp.
func createProtectedTimestampRecord(
	ctx context.Context,
	codec keys.SQLCodec,
	jobID jobspb.JobID,
	targets changefeedbase.Targets,
	resolved hlc.Timestamp,
) *ptpb.Record {
	ptsID := uuid.MakeV4()
	deprecatedSpansToProtect := makeSpansToProtect(codec, targets)
	targetToProtect := makeTargetToProtect(targets)

	log.VEventf(ctx, 2, "creating protected timestamp %v at %v", ptsID, resolved)
	return jobsprotectedts.MakeRecord(
		ptsID, int64(jobID), resolved, deprecatedSpansToProtect,
		jobsprotectedts.Jobs, targetToProtect)
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

func makeTargetToProtect(targets changefeedbase.Targets) *ptpb.Target {
	tablesToProtect := make(descpb.IDs, 0, targets.NumUniqueTables()+len(systemTablesToProtect))
	_ = targets.EachTableID(func(id descpb.ID) error {
		tablesToProtect = append(tablesToProtect, id)
		return nil
	})
	tablesToProtect = append(tablesToProtect, systemTablesToProtect...)
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

func makeSpansToProtect(codec keys.SQLCodec, targets changefeedbase.Targets) []roachpb.Span {
	spansToProtect := make([]roachpb.Span, 0, targets.NumUniqueTables()+len(systemTablesToProtect))
	addTablePrefix := func(id uint32) {
		tablePrefix := codec.TablePrefix(id)
		spansToProtect = append(spansToProtect, roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		})
	}
	_ = targets.EachTableID(func(id descpb.ID) error {
		addTablePrefix(uint32(id))
		return nil
	})
	for _, id := range systemTablesToProtect {
		addTablePrefix(uint32(id))
	}
	return spansToProtect
}
