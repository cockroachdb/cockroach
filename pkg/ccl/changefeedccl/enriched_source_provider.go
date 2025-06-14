// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"net"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/avro"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kcjsonschema"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

type enrichedSourceProviderOpts struct {
	updated, mvccTimestamp bool
}
type enrichedSourceData struct {
	jobID, sink,
	dbVersion, clusterName, sourceNodeLocality, nodeName, nodeID, clusterID string
	tableSchemaInfo map[descpb.ID]tableSchemaInfo
}
type tableSchemaInfo struct {
	tableName       string
	dbName          string
	schemaName      string
	primaryKeys     []string
	primaryKeysJSON json.JSON
}
type enrichedSourceProvider struct {
	opts              enrichedSourceProviderOpts
	sourceData        enrichedSourceData
	jsonPartialObject *json.PartialObject
	// jsonNonFixedData is a reusable map for non-fixed fields, which are the inputs to jsonPartialObject.NewObject.
	jsonNonFixedData map[string]json.JSON
}

func GetTableSchemaInfo(
	ctx context.Context, cfg *execinfra.ServerConfig, targets changefeedbase.Targets,
) (map[descpb.ID]tableSchemaInfo, error) {
	schemaInfo := make(map[descpb.ID]tableSchemaInfo)
	execCfg := cfg.ExecutorConfig.(*sql.ExecutorConfig)
	err := targets.EachTarget(func(target changefeedbase.Target) error {
		id := target.DescID
		td, dbd, sd, err := getDescriptors(ctx, execCfg, id)
		if err != nil {
			return err
		}

		primaryKeys := td.GetPrimaryIndex().IndexDesc().KeyColumnNames

		primaryKeysBuilder := json.NewArrayBuilder(len(primaryKeys))
		for _, key := range primaryKeys {
			primaryKeysBuilder.Add(json.FromString(key))
		}

		schemaInfo[id] = tableSchemaInfo{
			tableName:       td.GetName(),
			dbName:          dbd.GetName(),
			schemaName:      sd.GetName(),
			primaryKeys:     primaryKeys,
			primaryKeysJSON: primaryKeysBuilder.Build(),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return schemaInfo, nil
}

func newEnrichedSourceData(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	spec execinfrapb.ChangeAggregatorSpec,
	sink sinkType,
	schemaInfo map[descpb.ID]tableSchemaInfo,
) (enrichedSourceData, error) {
	var sourceNodeLocality, nodeName, nodeID string
	tiers := cfg.Locality.Tiers

	nodeLocalities := make([]string, 0, len(tiers))
	for _, t := range tiers {
		nodeLocalities = append(nodeLocalities, t.String())
	}
	sourceNodeLocality = strings.Join(nodeLocalities, ",")

	nodeInfo := cfg.ExecutorConfig.(*sql.ExecutorConfig).NodeInfo
	getPGURL := nodeInfo.PGURL
	pgurl, err := getPGURL(url.User(username.RootUser))
	if err != nil {
		return enrichedSourceData{}, err
	}
	parsedUrl, err := url.Parse(pgurl.String())
	if err != nil {
		return enrichedSourceData{}, err
	}
	host, _, err := net.SplitHostPort(parsedUrl.Host)
	if err == nil {
		nodeName = host
	}

	if optionalNodeID, ok := nodeInfo.NodeID.OptionalNodeID(); ok {
		nodeID = optionalNodeID.String()
	}

	return enrichedSourceData{
		jobID:              spec.JobID.String(),
		sink:               sink.String(),
		dbVersion:          build.GetInfo().Tag,
		clusterName:        cfg.ExecutorConfig.(*sql.ExecutorConfig).RPCContext.ClusterName(),
		clusterID:          nodeInfo.LogicalClusterID().String(),
		sourceNodeLocality: sourceNodeLocality,
		nodeName:           nodeName,
		nodeID:             nodeID,
		tableSchemaInfo:    schemaInfo,
	}, nil
}

func newEnrichedSourceProvider(
	opts changefeedbase.EncodingOptions, sourceData enrichedSourceData,
) (*enrichedSourceProvider, error) {
	jsonBase := map[string]json.JSON{
		fieldNameJobID:              json.FromString(sourceData.jobID),
		fieldNameChangefeedSink:     json.FromString(sourceData.sink),
		fieldNameDBVersion:          json.FromString(sourceData.dbVersion),
		fieldNameClusterName:        json.FromString(sourceData.clusterName),
		fieldNameClusterID:          json.FromString(sourceData.clusterID),
		fieldNameSourceNodeLocality: json.FromString(sourceData.sourceNodeLocality),
		fieldNameNodeName:           json.FromString(sourceData.nodeName),
		fieldNameNodeID:             json.FromString(sourceData.nodeID),
		fieldNameOrigin:             json.FromString(originCockroachDB),
	}

	var nonFixedJSONFields []string
	nonFixedDataIdx := map[string]int{}
	addNonFixedJSONfield := func(fieldName string) {
		nonFixedJSONFields = append(nonFixedJSONFields, fieldName)
		nonFixedDataIdx[fieldName] = len(nonFixedJSONFields) - 1
	}

	addNonFixedJSONfield(fieldNameDatabaseName)
	addNonFixedJSONfield(fieldNameSchemaName)
	addNonFixedJSONfield(fieldNameTableName)
	addNonFixedJSONfield(fieldNamePrimaryKeys)
	addNonFixedJSONfield(fieldNameTableID)

	if opts.MVCCTimestamps {
		addNonFixedJSONfield(fieldNameMVCCTimestamp)
	}
	if opts.UpdatedTimestamps {
		addNonFixedJSONfield(fieldNameUpdatedTSNS)
		addNonFixedJSONfield(fieldNameUpdatedTSHLC)
	}

	jpo, err := json.NewPartialObject(jsonBase, nonFixedJSONFields)
	if err != nil {
		return nil, err
	}

	return &enrichedSourceProvider{
		sourceData: sourceData,
		opts: enrichedSourceProviderOpts{
			mvccTimestamp: opts.MVCCTimestamps,
			updated:       opts.UpdatedTimestamps,
		},
		jsonPartialObject: jpo,
		jsonNonFixedData:  make(map[string]json.JSON, len(nonFixedJSONFields)),
	}, nil
}

func (p *enrichedSourceProvider) KafkaConnectJSONSchema() kcjsonschema.Schema {
	return kafkaConnectJSONSchema
}

// GetJSON returns a json object for the source data.
func (p *enrichedSourceProvider) GetJSON(
	updated cdcevent.Row, evCtx eventContext,
) (json.JSON, error) {
	clear(p.jsonNonFixedData)

	metadata := updated.Metadata
	tableID := metadata.TableID
	tableInfo, ok := p.sourceData.tableSchemaInfo[tableID]
	if !ok {
		return nil, errors.AssertionFailedf("table %d not found in tableSchemaInfo", tableID)
	}

	p.jsonNonFixedData[fieldNameDatabaseName] = json.FromString(tableInfo.dbName)
	p.jsonNonFixedData[fieldNameSchemaName] = json.FromString(tableInfo.schemaName)
	p.jsonNonFixedData[fieldNameTableName] = json.FromString(tableInfo.tableName)
	p.jsonNonFixedData[fieldNamePrimaryKeys] = tableInfo.primaryKeysJSON
	p.jsonNonFixedData[fieldNameTableID] = json.FromInt(int(metadata.TableID))

	if p.opts.mvccTimestamp {
		p.jsonNonFixedData[fieldNameMVCCTimestamp] = json.FromString(evCtx.mvcc.AsOfSystemTime())
	}
	if p.opts.updated {
		p.jsonNonFixedData[fieldNameUpdatedTSNS] = json.FromInt64(evCtx.updated.WallTime)
		p.jsonNonFixedData[fieldNameUpdatedTSHLC] = json.FromString(evCtx.updated.AsOfSystemTime())
	}
	return p.jsonPartialObject.NewObject(p.jsonNonFixedData)
}

// GetAvro returns an avro FunctionalRecord for the source data.
func (p *enrichedSourceProvider) GetAvro(
	row cdcevent.Row, schemaPrefix string, evCtx eventContext,
) (*avro.FunctionalRecord, error) {
	tableID := row.EventDescriptor.TableDescriptor().GetID()
	tableInfo, ok := p.sourceData.tableSchemaInfo[tableID]
	if !ok {
		return nil, errors.AssertionFailedf("table %d not found in tableSchemaInfo", tableID)
	}

	fromRow := func(row cdcevent.Row, dest map[string]any) {
		// If this is the first use of the avro record (ie the first row the encoder processed), set the fixed fields.
		if len(dest) == 0 {
			dest[fieldNameJobID] = goavro.Union(avro.SchemaTypeString, p.sourceData.jobID)
			dest[fieldNameChangefeedSink] = goavro.Union(avro.SchemaTypeString, p.sourceData.sink)
			dest[fieldNameDBVersion] = goavro.Union(avro.SchemaTypeString, p.sourceData.dbVersion)
			dest[fieldNameClusterName] = goavro.Union(avro.SchemaTypeString, p.sourceData.clusterName)
			dest[fieldNameClusterID] = goavro.Union(avro.SchemaTypeString, p.sourceData.clusterID)
			dest[fieldNameSourceNodeLocality] = goavro.Union(avro.SchemaTypeString, p.sourceData.sourceNodeLocality)
			dest[fieldNameNodeName] = goavro.Union(avro.SchemaTypeString, p.sourceData.nodeName)
			dest[fieldNameNodeID] = goavro.Union(avro.SchemaTypeString, p.sourceData.nodeID)
			dest[fieldNameOrigin] = goavro.Union(avro.SchemaTypeString, originCockroachDB)
		}

		dest[fieldNameDatabaseName] = goavro.Union(avro.SchemaTypeString, tableInfo.dbName)
		dest[fieldNameSchemaName] = goavro.Union(avro.SchemaTypeString, tableInfo.schemaName)
		dest[fieldNameTableName] = goavro.Union(avro.SchemaTypeString, tableInfo.tableName)
		dest[fieldNamePrimaryKeys] = goavro.Union(avro.SchemaTypeArray, tableInfo.primaryKeys)
		dest[fieldNameTableID] = goavro.Union(avro.SchemaTypeInt, int32(tableID))

		if p.opts.mvccTimestamp {
			dest[fieldNameMVCCTimestamp] = goavro.Union(avro.SchemaTypeString, evCtx.mvcc.AsOfSystemTime())
		}
		if p.opts.updated {
			dest[fieldNameUpdatedTSNS] = goavro.Union(avro.SchemaTypeLong, evCtx.updated.WallTime)
			dest[fieldNameUpdatedTSHLC] = goavro.Union(avro.SchemaTypeString, evCtx.updated.AsOfSystemTime())
		}
	}
	sourceDataSchema, err := avro.NewFunctionalRecord("source", schemaPrefix, avroFields, fromRow)
	if err != nil {
		return nil, err
	}
	return sourceDataSchema, nil
}

const (
	fieldNameJobID              = "job_id"
	fieldNameChangefeedSink     = "changefeed_sink"
	fieldNameDBVersion          = "db_version"
	fieldNameClusterName        = "cluster_name"
	fieldNameClusterID          = "cluster_id"
	fieldNameSourceNodeLocality = "source_node_locality"
	fieldNameNodeName           = "node_name"
	fieldNameNodeID             = "node_id"
	fieldNameMVCCTimestamp      = "mvcc_timestamp"
	fieldNameUpdatedTSNS        = "ts_ns"
	fieldNameUpdatedTSHLC       = "ts_hlc"
	fieldNameOrigin             = "origin"
	fieldNameDatabaseName       = "database_name"
	fieldNameSchemaName         = "schema_name"
	fieldNameTableName          = "table_name"
	fieldNamePrimaryKeys        = "primary_keys"
	fieldNameTableID            = "crdb_internal_table_id"
)

type fieldInfo struct {
	avroSchemaField    avro.SchemaField
	kafkaConnectSchema kcjsonschema.Schema
}

// allFieldInfo contains all the fields that are part of the source data, and is
// used to build the avro schema and the kafka connect json schema. Note that
// everything is nullable in avro for better backwards compatibility, whereas we
// use the optional flag in kafka connect more meaningfully.
var allFieldInfo = map[string]fieldInfo{
	fieldNameOrigin: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameOrigin,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameOrigin,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameChangefeedSink: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameChangefeedSink,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameChangefeedSink,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameJobID: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameJobID,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameJobID,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameDBVersion: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameDBVersion,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameDBVersion,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameClusterName: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameClusterName,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameClusterName,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameClusterID: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameClusterID,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameClusterID,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameSourceNodeLocality: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameSourceNodeLocality,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameSourceNodeLocality,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameNodeName: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameNodeName,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameNodeName,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameNodeID: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameNodeID,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameNodeID,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameMVCCTimestamp: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameMVCCTimestamp,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameMVCCTimestamp,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	fieldNameUpdatedTSNS: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameUpdatedTSNS,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeLong},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameUpdatedTSNS,
			TypeName: kcjsonschema.SchemaTypeInt64,
			Optional: true,
		},
	},
	fieldNameUpdatedTSHLC: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameUpdatedTSHLC,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameUpdatedTSHLC,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	fieldNameDatabaseName: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameDatabaseName,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameDatabaseName,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameSchemaName: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameSchemaName,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameSchemaName,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNameTableName: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameTableName,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameTableName,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: false,
		},
	},
	fieldNamePrimaryKeys: {
		avroSchemaField: avro.SchemaField{
			Name: fieldNamePrimaryKeys,
			SchemaType: []avro.SchemaType{
				avro.SchemaTypeNull,
				avro.ArrayType{SchemaType: avro.SchemaTypeArray, Items: avro.SchemaTypeString},
			},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNamePrimaryKeys,
			TypeName: kcjsonschema.SchemaTypeArray,
			Optional: false,
			Items:    &kcjsonschema.Schema{TypeName: kcjsonschema.SchemaTypeString},
		},
	},
	fieldNameTableID: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameTableID,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeInt},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameTableID,
			TypeName: kcjsonschema.SchemaTypeInt32,
			Optional: false,
		},
	},
}

// filled in by init() using allFieldInfo
var avroFields []*avro.SchemaField

// filled in by init() using allFieldInfo
var jsonFields []string

// filled in by init() using allFieldInfo
var kafkaConnectJSONSchema kcjsonschema.Schema

func init() {
	kcjFields := make([]kcjsonschema.Schema, 0, len(allFieldInfo))
	for _, info := range allFieldInfo {
		avroFields = append(avroFields, &info.avroSchemaField)
		kcjFields = append(kcjFields, info.kafkaConnectSchema)
		jsonFields = append(jsonFields, info.kafkaConnectSchema.Field)
	}

	kafkaConnectJSONSchema = kcjsonschema.Schema{
		Name:     "cockroachdb.source",
		TypeName: kcjsonschema.SchemaTypeStruct,
		Fields:   kcjFields,
		Optional: true,
	}
}

const originCockroachDB = "cockroachdb"

func getDescriptors(
	ctx context.Context, execCfg *sql.ExecutorConfig, tableID descpb.ID,
) (catalog.TableDescriptor, catalog.DatabaseDescriptor, catalog.SchemaDescriptor, error) {
	var tableDescriptor catalog.TableDescriptor
	var dbDescriptor catalog.DatabaseDescriptor
	var schemaDescriptor catalog.SchemaDescriptor
	var err error
	f := func(ctx context.Context, txn descs.Txn) error {
		byIDGetter := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get()
		tableDescriptor, err = byIDGetter.Table(ctx, tableID)
		if err != nil {
			return err
		}
		dbDescriptor, err = byIDGetter.Database(ctx, tableDescriptor.GetParentID())
		if err != nil {
			return err
		}
		schemaDescriptor, err = byIDGetter.Schema(ctx, tableDescriptor.GetParentSchemaID())
		if err != nil {
			return err
		}
		return nil
	}
	if err := execCfg.InternalDB.DescsTxn(ctx, f, isql.WithPriority(admissionpb.NormalPri)); err != nil {
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			err = changefeedbase.WithTerminalError(err)
		}
		return nil, nil, nil, err
	}
	return tableDescriptor, dbDescriptor, schemaDescriptor, nil
}
