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
	return &enrichedSourceProvider{sourceData: sourceData, opts: enrichedSourceProviderOpts{
		mvccTimestamp: opts.MVCCTimestamps,
		updated:       opts.UpdatedTimestamps,
	}}
}

func (p *enrichedSourceProvider) Schema() *avro.DataRecord {
	// TODO(#139655): Implement this.
	return nil
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

func (p *enrichedSourceProvider) GetAvro(row cdcevent.Row) ([]byte, error) {
	// TODO(#139655): Implement this.

	return nil, nil
}
