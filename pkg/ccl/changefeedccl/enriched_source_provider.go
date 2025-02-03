package changefeedccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

type enrichedEnveloptSourceProviderOpts struct {
	updated, diff, mvccTimestamp bool
}

type enrichedEnvelopeSourceProvider struct {
	opts enrichedEnveloptSourceProviderOpts

	jsonBuilder json.FixedKeysObjectBuilder
	// TODO(#139692): Add schema info support.
	// TODO(#139691): Add job info support.
	// TODO(#139690): Add node/cluster info support.
}

func newEnrichedEnvelopeSourceProvider(opts enrichedEnveloptSourceProviderOpts) *enrichedEnvelopeSourceProvider {
	return &enrichedEnvelopeSourceProvider{opts: opts}
}

// TODO(#139655): Implement this.
func (p *enrichedEnvelopeSourceProvider) Schema() *avroDataRecord {
	return nil
}

func (p *enrichedEnvelopeSourceProvider) GetJSON(row cdcevent.Row) json.JSON {
	// TODO(various): Add fields here
	return json.NewObjectBuilder(0).Build()
}

func (p *enrichedEnvelopeSourceProvider) GetAvro(row cdcevent.Row) ([]byte, error) {
	// TODO(#139655): Implement this.
	return nil, nil
}
