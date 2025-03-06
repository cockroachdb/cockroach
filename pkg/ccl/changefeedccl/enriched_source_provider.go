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
	return kcjSchema
}

func (p *enrichedSourceProvider) GetJSON(updated cdcevent.Row) (json.JSON, error) {
	// TODO(#141798): cache this. We'll need to cache a partial object since some fields are row-dependent (eg ts_ns).
	// TODO(various): Add fields here.
	keys := []string{
		"job_id", "db_version", "cluster_name", "cluster_id", "source_node_locality", "node_name", "node_id",
	}

	b, err := json.NewFixedKeysObjectBuilder(keys)
	if err != nil {
		return nil, err
	}

	if err := b.Set("job_id", json.FromString(p.sourceData.jobID)); err != nil {
		return nil, err
	}
	if err := b.Set("db_version", json.FromString(p.sourceData.dbVersion)); err != nil {
		return nil, err
	}
	if err := b.Set("cluster_name", json.FromString(p.sourceData.clusterName)); err != nil {
		return nil, err
	}
	if err := b.Set("cluster_id", json.FromString(p.sourceData.clusterID)); err != nil {
		return nil, err
	}
	if err := b.Set("source_node_locality", json.FromString(p.sourceData.sourceNodeLocality)); err != nil {
		return nil, err
	}
	if err := b.Set("node_name", json.FromString(p.sourceData.nodeName)); err != nil {
		return nil, err
	}
	if err := b.Set("node_id", json.FromString(p.sourceData.nodeID)); err != nil {
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

type fieldInfo struct {
	avroSchemaField    avro.SchemaField
	kafkaConnectSchema kcjsonschema.Schema
}

var allFieldInfo = map[string]fieldInfo{
	"job_id": {
		avroSchemaField: avro.SchemaField{
			Name:       "job_id",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "job_id",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	"db_version": {
		avroSchemaField: avro.SchemaField{
			Name:       "db_version",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "db_version",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	"cluster_name": {
		avroSchemaField: avro.SchemaField{
			Name:       "cluster_name",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "cluster_name",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	"cluster_id": {
		avroSchemaField: avro.SchemaField{
			Name:       "cluster_id",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "cluster_id",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	"source_node_locality": {
		avroSchemaField: avro.SchemaField{
			Name:       "source_node_locality",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "source_node_locality",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	"node_name": {
		avroSchemaField: avro.SchemaField{
			Name:       "node_name",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "node_name",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
	"node_id": {
		avroSchemaField: avro.SchemaField{
			Name:       "node_id",
			SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString},
		},
		kafkaConnectSchema: kcjsonschema.Schema{
			Field:    "node_id",
			TypeName: kcjsonschema.SchemaTypeString,
			Optional: true,
		},
	},
}

// filled in by init() using allFieldInfo
var avroFields []*avro.SchemaField

// filled in by init() using allFieldInfo
var kcjSchema kcjsonschema.Schema

func init() {
	kcjFields := make([]kcjsonschema.Schema, 0, len(allFieldInfo))
	for _, info := range allFieldInfo {
		avroFields = append(avroFields, &info.avroSchemaField)
		kcjFields = append(kcjFields, info.kafkaConnectSchema)
	}

	kcjSchema = kcjsonschema.Schema{
		Name:     "cockroachdb.source",
		TypeName: kcjsonschema.SchemaTypeStruct,
		Fields:   kcjFields,
		Optional: true,
	}
}
