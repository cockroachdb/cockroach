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
	jobID,
	dbVersion, clusterName, sourceNodeLocality, nodeName, nodeID, clusterID string
	// TODO(#139692): Add schema info support.
	// TODO(#139691): Add job info support.
	// TODO(#139690): Add node/cluster info support.
}
type enrichedSourceProvider struct {
	opts       enrichedSourceProviderOpts
	sourceData enrichedSourceData
}

func newEnrichedSourceData(
	ctx context.Context, cfg *execinfra.ServerConfig, spec execinfrapb.ChangeAggregatorSpec,
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
) *enrichedSourceProvider {
	return &enrichedSourceProvider{
		sourceData: sourceData,
		opts: enrichedSourceProviderOpts{
			mvccTimestamp: opts.MVCCTimestamps,
			updated:       opts.UpdatedTimestamps,
		},
	}
}

func (p *enrichedSourceProvider) avroSourceFunction(row cdcevent.Row) (map[string]any, error) {
	// TODO(#141798): cache this. We'll need to cache a partial object since some fields are row-dependent (eg ts_ns).
	return map[string]any{
		"job_id":               goavro.Union(avro.SchemaTypeString, p.sourceData.jobID),
		"db_version":           goavro.Union(avro.SchemaTypeString, p.sourceData.dbVersion),
		"cluster_name":         goavro.Union(avro.SchemaTypeString, p.sourceData.clusterName),
		"cluster_id":           goavro.Union(avro.SchemaTypeString, p.sourceData.clusterID),
		"source_node_locality": goavro.Union(avro.SchemaTypeString, p.sourceData.sourceNodeLocality),
		"node_name":            goavro.Union(avro.SchemaTypeString, p.sourceData.nodeName),
		"node_id":              goavro.Union(avro.SchemaTypeString, p.sourceData.nodeID),
	}, nil
}

func (p *enrichedSourceProvider) KafkaConnectJSONSchema() kcjsonschema.Schema {
	return kafkaConnectJSONSchema
}

func (p *enrichedSourceProvider) GetJSON(updated cdcevent.Row) (json.JSON, error) {
	// TODO(#141798): cache this. We'll need to cache a partial object since some fields are row-dependent (eg ts_ns).
	// TODO(various): Add fields here.
	keys := []string{
		fieldNameJobID, fieldNameDBVersion, fieldNameClusterName, fieldNameClusterID,
		fieldNameSourceNodeLocality, fieldNameNodeName, fieldNameNodeID,
	}

	b, err := json.NewFixedKeysObjectBuilder(keys)
	if err != nil {
		return nil, err
	}

	if err := b.Set(fieldNameJobID, json.FromString(p.sourceData.jobID)); err != nil {
		return nil, err
	}
	if err := b.Set(fieldNameDBVersion, json.FromString(p.sourceData.dbVersion)); err != nil {
		return nil, err
	}
	if err := b.Set(fieldNameClusterName, json.FromString(p.sourceData.clusterName)); err != nil {
		return nil, err
	}
	if err := b.Set(fieldNameClusterID, json.FromString(p.sourceData.clusterID)); err != nil {
		return nil, err
	}
	if err := b.Set(fieldNameSourceNodeLocality, json.FromString(p.sourceData.sourceNodeLocality)); err != nil {
		return nil, err
	}
	if err := b.Set(fieldNameNodeName, json.FromString(p.sourceData.nodeName)); err != nil {
		return nil, err
	}
	if err := b.Set(fieldNameNodeID, json.FromString(p.sourceData.nodeID)); err != nil {
		return nil, err
	}

	return b.Build()
}

func (p *enrichedSourceProvider) GetAvro(
	row cdcevent.Row, schemaPrefix string,
) (*avro.FunctionalRecord, error) {
	sourceDataSchema, err := avro.NewFunctionalRecord("source", schemaPrefix, avroFields, p.avroSourceFunction)
	if err != nil {
		return nil, err
	}
	return sourceDataSchema, nil
}

const (
	fieldNameJobID              = "job_id"
	fieldNameDBVersion          = "db_version"
	fieldNameClusterName        = "cluster_name"
	fieldNameClusterID          = "cluster_id"
	fieldNameSourceNodeLocality = "source_node_locality"
	fieldNameNodeName           = "node_name"
	fieldNameNodeID             = "node_id"
)

type fieldInfo struct {
	avroSchemaField    avro.SchemaField
	kafkaConnectSchema kcjsonschema.Schema
}

var allFieldInfo = map[string]fieldInfo{
	fieldNameJobID: {
		avroSchemaField: avro.SchemaField{
			Name:       fieldNameJobID,
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    fieldNameJobID,
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
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
			Optional: true,
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
			Optional: true,
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
			Optional: true,
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
			Optional: true,
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
			Optional: true,
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
			Optional: true,
		},
	},
}

// filled in by init() using allFieldInfo
var avroFields []*avro.SchemaField

// filled in by init() using allFieldInfo
var kafkaConnectJSONSchema kcjsonschema.Schema

func init() {
	kcjFields := make([]kcjsonschema.Schema, 0, len(allFieldInfo))
	for _, info := range allFieldInfo {
		avroFields = append(avroFields, &info.avroSchemaField)
		kcjFields = append(kcjFields, info.kafkaConnectSchema)
	}

	kafkaConnectJSONSchema = kcjsonschema.Schema{
		Name:     "cockroachdb.source",
		TypeName: kcjsonschema.SchemaTypeStruct,
		Fields:   kcjFields,
		Optional: true,
	}
}
