// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"strings"

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
	"github.com/cockroachdb/errors"
)

const (
	jsonMetaSentinel = `__crdb__`
)

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the
// sink.
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink Sink, resolved hlc.Timestamp,
) error {
	// TODO(dan): Emit more fine-grained (table level) resolved
	// timestamps.
	if err := sink.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `resolved %s`, resolved)
	}
	return nil
}

// createProtectedTimestampRecord will create a record to protect the spans for
// this changefeed at the resolved timestamp. The progress struct will be
// updated to refer to this new protected timestamp record.
func createProtectedTimestampRecord(
	ctx context.Context,
	codec keys.SQLCodec,
	jobID jobspb.JobID,
	targets []jobspb.ChangefeedTargetSpecification,
	resolved hlc.Timestamp,
	progress *jobspb.ChangefeedProgress,
) *ptpb.Record {
	progress.ProtectedTimestampRecord = uuid.MakeV4()
	deprecatedSpansToProtect := makeSpansToProtect(codec, targets)
	targetToProtect := makeTargetToProtect(targets)

	log.VEventf(ctx, 2, "creating protected timestamp %v at %v", progress.ProtectedTimestampRecord, resolved)
	return jobsprotectedts.MakeRecord(
		progress.ProtectedTimestampRecord, int64(jobID), resolved, deprecatedSpansToProtect,
		jobsprotectedts.Jobs, targetToProtect)
}

func makeTargetToProtect(targets []jobspb.ChangefeedTargetSpecification) *ptpb.Target {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	tablesToProtect := make(descpb.IDs, 0, len(targets)+1)
	for _, t := range targets {
		tablesToProtect = append(tablesToProtect, t.TableID)
	}
	tablesToProtect = append(tablesToProtect, keys.DescriptorTableID)
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

func makeSpansToProtect(
	codec keys.SQLCodec, targets []jobspb.ChangefeedTargetSpecification,
) []roachpb.Span {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	spansToProtect := make([]roachpb.Span, 0, len(targets)+1)
	addTablePrefix := func(id uint32) {
		tablePrefix := codec.TablePrefix(id)
		spansToProtect = append(spansToProtect, roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		})
	}
	for _, t := range targets {
		addTablePrefix(uint32(t.TableID))
	}
	addTablePrefix(keys.DescriptorTableID)
	return spansToProtect
}

// initialScanTypeFromOpts determines the type of initial scan the changefeed
// should perform on the first run given the options provided from the user
func initialScanTypeFromOpts(opts map[string]string) (changefeedbase.InitialScanType, error) {
	_, cursor := opts[changefeedbase.OptCursor]
	initialScanType, initialScanSet := opts[changefeedbase.OptInitialScan]
	_, initialScanOnlySet := opts[changefeedbase.OptInitialScanOnly]
	_, noInitialScanSet := opts[changefeedbase.OptNoInitialScan]

	if initialScanSet && noInitialScanSet {
		return changefeedbase.InitialScan, errors.Errorf(
			`cannot specify both %s and %s`, changefeedbase.OptInitialScan,
			changefeedbase.OptNoInitialScan)
	}

	if initialScanSet && initialScanOnlySet {
		return changefeedbase.InitialScan, errors.Errorf(
			`cannot specify both %s and %s`, changefeedbase.OptInitialScan,
			changefeedbase.OptInitialScanOnly)
	}

	if noInitialScanSet && initialScanOnlySet {
		return changefeedbase.InitialScan, errors.Errorf(
			`cannot specify both %s and %s`, changefeedbase.OptInitialScanOnly,
			changefeedbase.OptNoInitialScan)
	}

	if initialScanSet {
		const opt = changefeedbase.OptInitialScan
		switch strings.ToLower(initialScanType) {
		case ``, `yes`:
			return changefeedbase.InitialScan, nil
		case `no`:
			return changefeedbase.NoInitialScan, nil
		case `only`:
			return changefeedbase.OnlyInitialScan, nil
		default:
			return changefeedbase.InitialScan, errors.Errorf(
				`unknown %s: %s`, opt, initialScanType)
		}
	}

	if initialScanOnlySet {
		return changefeedbase.OnlyInitialScan, nil
	}

	if noInitialScanSet {
		return changefeedbase.NoInitialScan, nil
	}

	// If we reach this point, this implies that the user did not specify any initial scan
	// options. In this case the default behaviour is to perform an initial scan if the
	// cursor is not specified.
	if !cursor {
		return changefeedbase.InitialScan, nil
	}

	return changefeedbase.NoInitialScan, nil
}
