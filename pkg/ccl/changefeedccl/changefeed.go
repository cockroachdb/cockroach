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
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/gogo/protobuf/jsonpb"
)

// ChangefeedConfig provides a version-agnostic wrapper around jobspb.ChangefeedDetails.
type ChangefeedConfig struct {
	// SinkURI specifies the destination of changefeed events.
	SinkURI string
	// Opts are the WITH options for this changefeed.
	Opts changefeedbase.StatementOptions
	// ScanTime (also called StatementTime) is the timestamp at which an initial_scan
	// (if any) is performed.
	ScanTime hlc.Timestamp
	// EndTime, if nonzero, ends the changefeed once all events up to the given time
	// have been emitted.
	EndTime hlc.Timestamp
	// Targets uniquely identifies each target being watched.
	Targets changefeedbase.Targets
}

// makeChangefeedConfigFromJobDetails creates a ChangefeedConfig struct from any
// version of the ChangefeedDetails protobuf.
func makeChangefeedConfigFromJobDetails(d jobspb.ChangefeedDetails) ChangefeedConfig {
	return ChangefeedConfig{
		SinkURI:  d.SinkURI,
		Opts:     changefeedbase.MakeStatementOptions(d.Opts),
		ScanTime: d.StatementTime,
		EndTime:  d.EndTime,
		Targets:  AllTargets(d),
	}
}

// AllTargets gets all the targets listed in a ChangefeedDetails,
// from the statement time name map in old protos
// or the TargetSpecifications in new ones.
func AllTargets(cd jobspb.ChangefeedDetails) (targets changefeedbase.Targets) {
	// TODO: Use a version gate for this once we have CDC version gates
	if len(cd.TargetSpecifications) > 0 {
		for _, ts := range cd.TargetSpecifications {
			if ts.TableID > 0 {
				if ts.StatementTimeName == "" {
					ts.StatementTimeName = cd.Tables[ts.TableID].StatementTimeName
				}
				targets.Add(changefeedbase.Target{
					Type:              ts.Type,
					TableID:           ts.TableID,
					FamilyName:        ts.FamilyName,
					StatementTimeName: changefeedbase.StatementTimeName(ts.StatementTimeName),
				})
			}
		}
	} else {
		for id, t := range cd.Tables {
			targets.Add(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				TableID:           id,
				StatementTimeName: changefeedbase.StatementTimeName(t.StatementTimeName),
			})
		}
	}
	return
}

const (
	jsonMetaSentinel = `__crdb__`
)

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the
// sink.
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink ResolvedTimestampSink, resolved hlc.Timestamp,
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
	targets changefeedbase.Targets,
	resolved hlc.Timestamp,
	expiration time.Duration,
	progress *jobspb.ChangefeedProgress,
) *ptpb.Record {
	progress.ProtectedTimestampRecord = uuid.MakeV4()
	deprecatedSpansToProtect := makeSpansToProtect(codec, targets)
	targetToProtect := makeTargetToProtect(targets)
	targetToProtect.Expiration = expiration

	log.VEventf(ctx, 2, "creating protected timestamp %v at %v", progress.ProtectedTimestampRecord, resolved)
	return jobsprotectedts.MakeRecord(
		progress.ProtectedTimestampRecord, int64(jobID), resolved, deprecatedSpansToProtect,
		jobsprotectedts.Jobs, targetToProtect)
}

func makeTargetToProtect(targets changefeedbase.Targets) *ptpb.Target {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	tablesToProtect := make(descpb.IDs, 0, targets.NumUniqueTables()+1)
	_ = targets.EachTableID(func(id descpb.ID) error {
		tablesToProtect = append(tablesToProtect, id)
		return nil
	})
	tablesToProtect = append(tablesToProtect, keys.DescriptorTableID)
	return ptpb.MakeSchemaObjectsTarget(tablesToProtect)
}

func makeSpansToProtect(codec keys.SQLCodec, targets changefeedbase.Targets) []roachpb.Span {
	// NB: We add 1 because we're also going to protect system.descriptors.
	// We protect system.descriptors because a changefeed needs all of the history
	// of table descriptors to version data.
	spansToProtect := make([]roachpb.Span, 0, targets.NumUniqueTables()+1)
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
	addTablePrefix(keys.DescriptorTableID)
	return spansToProtect
}

// Inject the change feed details marshal logic into the jobspb package.
func init() {
	jobspb.ChangefeedDetailsMarshaler = func(m *jobspb.ChangefeedDetails, marshaller *jsonpb.Marshaler) ([]byte, error) {
		if protoreflect.ShouldRedact(marshaller) {
			var err error
			m.SinkURI, err = cloud.SanitizeExternalStorageURI(m.SinkURI, nil)
			if err != nil {
				return nil, err
			}
		}
		return json.Marshal(m)
	}
}
