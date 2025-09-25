// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
func makeChangefeedConfigFromJobDetails(
	d jobspb.ChangefeedDetails, targets changefeedbase.Targets,
) (ChangefeedConfig, error) {
	return ChangefeedConfig{
		SinkURI:  d.SinkURI,
		Opts:     changefeedbase.MakeStatementOptions(d.Opts),
		ScanTime: d.StatementTime,
		EndTime:  d.EndTime,
		Targets:  targets,
	}, nil
}

// AllTargets gets all the targets listed in a ChangefeedDetails,
// from the statement time name map in old protos
// or the TargetSpecifications in new ones.
func AllTargets(
	ctx context.Context, cd jobspb.ChangefeedDetails, execCfg *sql.ExecutorConfig,
) (changefeedbase.Targets, error) {
	targets := changefeedbase.Targets{}
	var err error

	// TODO: Use a version gate for this once we have CDC version gates
	if len(cd.TargetSpecifications) > 0 {
		for _, ts := range cd.TargetSpecifications {
			if ts.DescID > 0 {
				switch ts.Type {
				case jobspb.ChangefeedTargetSpecification_DATABASE:
					if len(cd.TargetSpecifications) > 1 {
						return changefeedbase.Targets{}, errors.AssertionFailedf("database-level changefeed is not supported with multiple targets")
					}
					targets, err = getTargetsFromDatabaseSpec(ctx, ts, execCfg)
					if err != nil {
						return changefeedbase.Targets{}, err
					}
				case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
					jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
					jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
					if ts.StatementTimeName == "" {
						ts.StatementTimeName = cd.Tables[ts.DescID].StatementTimeName
					}
					targets.Add(changefeedbase.Target{
						Type:              ts.Type,
						DescID:            ts.DescID,
						FamilyName:        ts.FamilyName,
						StatementTimeName: changefeedbase.StatementTimeName(ts.StatementTimeName),
					})
				default:
					return changefeedbase.Targets{}, errors.AssertionFailedf("unsupported target type: %s", ts.Type)
				}
			}
		}
	} else {
		for id, t := range cd.Tables {
			targets.Add(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				DescID:            id,
				StatementTimeName: changefeedbase.StatementTimeName(t.StatementTimeName),
			})
		}
	}
	return targets, err
}

func getTargetsFromDatabaseSpec(
	ctx context.Context, ts jobspb.ChangefeedTargetSpecification, execCfg *sql.ExecutorConfig,
) (targets changefeedbase.Targets, err error) {
	err = sql.DescsTxn(ctx, execCfg, func(
		ctx context.Context, txn isql.Txn, descs *descs.Collection,
	) error {
		databaseDescriptor, err := descs.ByIDWithLeased(txn.KV()).Get().Database(ctx, ts.DescID)
		if err != nil {
			return err
		}
		tableDescToSchemaName := make(map[catalog.TableDescriptor]string)
		tables, err := descs.GetAllTablesInDatabase(ctx, txn.KV(), databaseDescriptor)
		if err != nil {
			return err
		}
		dbName := databaseDescriptor.GetName()
		if ts.FilterList == nil ||
			ts.FilterList.FilterType == jobspb.FilterList_EXCLUDE_TABLES {
			for _, desc := range tables.OrderedDescriptors() {
				tableDesc, ok := desc.(catalog.TableDescriptor)
				if !ok {
					return errors.AssertionFailedf("expected table descriptor, got %T", desc)
				}
				// Skip virtual tables
				if !tableDesc.IsPhysicalTable() {
					continue
				}
				if ts.FilterList != nil && ts.FilterList.FilterType == jobspb.FilterList_EXCLUDE_TABLES {
					if _, ok := tableDescToSchemaName[tableDesc]; !ok {
						schemaID := tableDesc.GetParentSchemaID()
						schema, err := descs.ByIDWithLeased(txn.KV()).Get().Schema(ctx, schemaID)
						if err != nil {
							return err
						}
						tableDescToSchemaName[tableDesc] = schema.GetName()
					}
					fullyQualifiedTableName := fmt.Sprintf(
						"%s.%s.%s", dbName, tableDescToSchemaName[tableDesc], tableDesc.GetName())
					if _, ok := ts.FilterList.Tables[fullyQualifiedTableName]; ok {
						continue
					}
				}
				var tableType jobspb.ChangefeedTargetSpecification_TargetType
				if len(tableDesc.GetFamilies()) == 1 {
					tableType = jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY
				} else {
					tableType = jobspb.ChangefeedTargetSpecification_EACH_FAMILY
				}

				targets.Add(changefeedbase.Target{
					Type:              tableType,
					DescID:            desc.GetID(),
					StatementTimeName: changefeedbase.StatementTimeName(desc.GetName()),
				})
			}
		} else if ts.FilterList.FilterType == jobspb.FilterList_INCLUDE_TABLES {
			for name, _ := range ts.FilterList.Tables {
				tn, err := parser.ParseTableName(name)
				if err != nil {
					return err
				}

				// Use ts.DescID instead of tn.Catalog() because the database name
				// could have been renamed.
				schemaID, err := descs.LookupSchemaID(ctx, txn.KV(), ts.DescID, tn.Schema())
				if err != nil {
					return err
				}
				if schemaID == descpb.InvalidID {
					continue
				}

				tableID, err := descs.LookupObjectID(ctx, txn.KV(), ts.DescID, schemaID, tn.Object())
				if err != nil {
					return err
				}
				if tableID == descpb.InvalidID {
					continue
				}

				desc, err := descs.ByIDWithLeased(txn.KV()).Get().Table(ctx, tableID)
				if err != nil {
					return err
				}

				var tableType jobspb.ChangefeedTargetSpecification_TargetType
				if len(desc.GetFamilies()) == 1 {
					tableType = jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY
				} else {
					tableType = jobspb.ChangefeedTargetSpecification_EACH_FAMILY
				}

				targets.Add(changefeedbase.Target{
					Type:              tableType,
					DescID:            tableID,
					StatementTimeName: changefeedbase.StatementTimeName(desc.GetName()),
				})
			}
		} else {
			return errors.AssertionFailedf("invalid changefeed filter type")
		}
		return nil
	})
	return targets, err
}

const (
	// metaSentinel is a key or prefix used to mark metadata fields or columns
	// into rows returned by an encoder.
	metaSentinel = `__crdb__`
)

// emitResolvedTimestamp emits a changefeed-level resolved timestamp to the
// sink.
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink ResolvedTimestampSink, resolved hlc.Timestamp,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.emit_resolved_timestamp")
	defer sp.Finish()

	// TODO(dan): Emit more fine-grained (table level) resolved
	// timestamps.
	if err := sink.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return err
	}
	if log.V(2) {
		log.Changefeed.Infof(ctx, `resolved %s`, resolved)
	}
	return nil
}

// Inject the change feed details marshal logic into the jobspb package.
func init() {
	jobspb.ChangefeedDetailsMarshaler = func(m *jobspb.ChangefeedDetails, marshaller *jsonpb.Marshaler) ([]byte, error) {
		if protoreflect.ShouldRedact(marshaller) {
			var err error
			// Redacts user sensitive information from sinkURI.
			m.SinkURI, err = cloud.SanitizeExternalStorageURI(m.SinkURI, []string{
				changefeedbase.SinkParamSASLPassword,
				changefeedbase.SinkParamCACert,
				changefeedbase.SinkParamClientKey,
				changefeedbase.SinkParamClientCert,
				changefeedbase.SinkParamConfluentAPISecret,
				changefeedbase.SinkParamAzureAccessKey,
				changefeedbase.SinkParamAzureAccessKeyCamel,
			})
			if err != nil {
				return nil, err
			}
		}
		return json.Marshal(m)
	}
}
