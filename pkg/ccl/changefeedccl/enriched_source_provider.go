// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/avro"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

type enrichedEnvelopeSourceProviderOpts struct {
	updated, mvccTimestamp bool
}
type enrichedEnvelopeSourceDataSource struct {
	jobId string
}
type enrichedEnvelopeSourceProvider struct {
	opts       enrichedEnvelopeSourceProviderOpts
	sourceData enrichedEnvelopeSourceDataSource
	// TODO(#139692): Add schema info support.
	// TODO(#139691): Add job info support.
	// TODO(#139690): Add node/cluster info support.
}

func newEnrichedEnvelopeSourceProvider(
	opts enrichedEnvelopeSourceProviderOpts, sourceData enrichedEnvelopeSourceDataSource,
) *enrichedEnvelopeSourceProvider {
	return &enrichedEnvelopeSourceProvider{sourceData: sourceData, opts: opts}
}

func (p *enrichedEnvelopeSourceProvider) Schema() *avro.DataRecord {
	// TODO(#139655): Implement this.
	return nil
}

func (p *enrichedEnvelopeSourceProvider) GetJSON(row cdcevent.Row) (json.JSON, error) {
	// TODO(various): Add fields here.
	keys := []string{"jobId"}
	b, err := json.NewFixedKeysObjectBuilder(keys)
	if err != nil {
		return nil, err
	}

	if err := b.Set("jobId", json.FromString(p.sourceData.jobId)); err != nil {
		return nil, err
	}

	return b.Build()
}

func (p *enrichedEnvelopeSourceProvider) GetAvro(row cdcevent.Row) ([]byte, error) {
	// TODO(#139655): Implement this.

	return nil, nil
}
