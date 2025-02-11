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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/avro"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
		dbVersion:          cfg.Settings.Version.ActiveVersion(ctx).String(),
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

func (p *enrichedSourceProvider) getAvroFields() []*avro.SchemaField {
	return []*avro.SchemaField{
		{Name: "job_id", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
		{Name: "db_version", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
		{Name: "cluster_name", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
		{Name: "cluster_id", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
		{Name: "source_node_locality", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
		{Name: "node_name", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
		{Name: "node_id", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
	}
}

func (p *enrichedSourceProvider) Schema() (*avro.FunctionalRecord, error) {
	rec, err := avro.NewFunctionalRecord("source", "" /* namespace */, p.getAvroFields(), p.avroSourceFunction)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (p *enrichedSourceProvider) GetJSON(
	updated cdcevent.Row, updatedTs hlc.Timestamp, mvccTs hlc.Timestamp,
) (json.JSON, error) {
	// TODO(various): Add fields here.
	keys := []string{
		"job_id", "db_version", "cluster_name", "cluster_id", "source_node_locality", "node_name", "node_id",
	}

	if p.opts.updated {
		keys = append(keys, "ts_hlc")
		keys = append(keys, "ts_ns")
	}
	if p.opts.mvccTimestamp {
		keys = append(keys, "mvcc_timestamp")
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
	sourceDataSchema, err := avro.NewFunctionalRecord("source", schemaPrefix, p.getAvroFields(), p.avroSourceFunction)
	if err != nil {
		return nil, err
	}
	return sourceDataSchema, nil
}
