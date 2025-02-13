// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/avro"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/linkedin/goavro/v2"
)

type enrichedSourceProviderOpts struct {
	updated, mvccTimestamp bool
}
type enrichedSourceData struct {
	jobId string
	// TODO(#139692): Add schema info support.
	// TODO(#139691): Add job info support.
	// TODO(#139690): Add node/cluster info support.
}
type enrichedSourceProvider struct {
	opts       enrichedSourceProviderOpts
	sourceData enrichedSourceData
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
		"job_id": goavro.Union(avro.SchemaTypeString, p.sourceData.jobId),
	}, nil
}

func (p *enrichedSourceProvider) getAvroFields() []*avro.SchemaField {
	return []*avro.SchemaField{
		{Name: "job_id", SchemaType: []avro.SchemaType{avro.SchemaTypeNull, avro.SchemaTypeString}},
	}
}

func (p *enrichedSourceProvider) Schema() (*avro.FunctionalRecord, error) {
	rec, err := avro.NewFunctionalRecord("source", "" /* namespace */, p.getAvroFields(), p.avroSourceFunction)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func (p *enrichedSourceProvider) GetJSON(row cdcevent.Row) (json.JSON, error) {
	// TODO(various): Add fields here.
	keys := []string{"job_id"}
	b, err := json.NewFixedKeysObjectBuilder(keys)
	if err != nil {
		return nil, err
	}

	if err := b.Set("job_id", json.FromString(p.sourceData.jobId)); err != nil {
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
