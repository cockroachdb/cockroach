package changefeedccl

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/avro"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

type enrichedEnvelopeSourceProviderOpts struct {
	updated, diff, mvccTimestamp bool
	sourceClusterName            string
}
type enrichedEnvelopeSourceProvider struct {
	opts        enrichedEnvelopeSourceProviderOpts
	jsonBuilder json.FixedKeysObjectBuilder
	// TODO(#139692): Add schema info support.
	// TODO(#139691): Add job info support.
	// TODO(#139690): Add node/cluster info support.
}

func newEnrichedEnvelopeSourceProvider(
	opts enrichedEnvelopeSourceProviderOpts,
) *enrichedEnvelopeSourceProvider {
	return &enrichedEnvelopeSourceProvider{opts: opts}
}

func (p *enrichedEnvelopeSourceProvider) Schema() *avro.DataRecord {
	// TODO(#139655): Implement this.
	return nil
}
func (p *enrichedEnvelopeSourceProvider) GetJSON(row cdcevent.Row) json.JSON {
	// TODO(various): Add fields here.
	// this isn't quite it, placeholder for now
	return json.FromString(fmt.Sprintf(`{"foo": %s}`, p.opts.sourceClusterName))
}
func (p *enrichedEnvelopeSourceProvider) GetAvro(row cdcevent.Row) ([]byte, error) {
	// TODO(#139655): Implement this.

	return nil, nil
}
