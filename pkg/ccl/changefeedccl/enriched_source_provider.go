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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/linkedin/goavro/v2"
)

type enrichedSourceProviderOpts struct {
	updated, mvccTimestamp bool
}
type enrichedSourceData struct {
	jobID, sink,
	dbVersion, clusterName, sourceNodeLocality, nodeName, nodeID, clusterID string
	// TODO(#139692): Add schema info support.
	// TODO(#139691): Add job info support.
	// TODO(#139690): Add node/cluster info support.
}
type enrichedSourceProvider struct {
	opts              enrichedSourceProviderOpts
	sourceData        enrichedSourceData
	jsonPartialObject *json.PartialObject
	// jsonNonFixedData is a reusable map for non-fixed fields, which are the inputs to jsonPartialObject.NewObject.
	jsonNonFixedData map[string]json.JSON
}

func newEnrichedSourceData(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	spec execinfrapb.ChangeAggregatorSpec,
	sink sinkType,
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
	}

	var nonFixedJSONFields []string
	nonFixedDataIdx := map[string]int{}
	if opts.MVCCTimestamps {
		nonFixedJSONFields = append(nonFixedJSONFields, fieldNameMVCCTimestamp)
		nonFixedDataIdx[fieldNameMVCCTimestamp] = len(nonFixedJSONFields) - 1
	}
	// TODO(#139661): Add other non fixed fields.

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
func (p *enrichedSourceProvider) GetJSON(updated cdcevent.Row) (json.JSON, error) {
	// TODO(#139661): Add other non fixed fields.
	clear(p.jsonNonFixedData)
	if p.opts.mvccTimestamp {
		p.jsonNonFixedData[fieldNameMVCCTimestamp] = json.FromString(updated.MvccTimestamp.AsOfSystemTime())
	}
	return p.jsonPartialObject.NewObject(p.jsonNonFixedData)
}

// GetAvro returns an avro FunctionalRecord for the source data.
func (p *enrichedSourceProvider) GetAvro(
	row cdcevent.Row, schemaPrefix string,
) (*avro.FunctionalRecord, error) {
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
		}

		if p.opts.mvccTimestamp {
			dest[fieldNameMVCCTimestamp] = goavro.Union(avro.SchemaTypeString, row.MvccTimestamp.AsOfSystemTime())
		}
		// TODO(#139661): Add other non fixed fields.
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
