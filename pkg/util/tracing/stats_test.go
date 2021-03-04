package tracing_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/gogo/protobuf/types"
)

func BenchmarkSpanStats(b *testing.B) {
	tr := tracing.NewTracer()
	for _, autoCollection := range []bool{false, true} {
		for _, withStats := range []int{0, 1, 10} {
			b.Run(fmt.Sprintf("autoCollection=%t,stats=%d", autoCollection, withStats), func(b *testing.B) {
				var cs execinfrapb.ComponentStats // intentionally reused to simulate pooling
				for i := 0; i < b.N; i++ {
					root := tr.StartSpan("root", tracing.WithForceRealSpan())
					var sp *tracing.Span
					if !autoCollection {
						sp = tr.StartSpan("child", tracing.WithParentAndManualCollection(root.Meta()))
					} else {
						sp = tr.StartSpan("child", tracing.WithParentAndAutoCollection(root))
					}
					for j := 0; j < withStats; j++ {
						stats := execinfrapb.ComponentStats{}
						stats.Component.ID = int32(j)
						sp.RecordStructured(&stats)
					}
					sp.Finish()
					if !autoCollection {
						rec := sp.GetRecording()
						if len(rec) != 1 {
							b.Fatal(rec)
						}
						// The SQL code, when the whole plan is on the gateway, as is the case
						// for the most performance-sensitive case here, still remarshals the
						// recording which is expensive. With remarshal=false we see that it
						// shaves off a couple 100 ns, so not a ton, with remarshal=true it's
						// much more expensive since un/marshalling the ComponentStats drives
						// overall cost and is a part of this.
						const remarshal = true
						if remarshal {
							bt, err := protoutil.Marshal(&rec[0])
							if err != nil {
								b.Fatal(err)
							}
							rec2 := []tracingpb.RecordedSpan{{}}
							if err := protoutil.Unmarshal(bt, &rec2[0]); err != nil {
								b.Fatal(err)
							}
							rec = rec2
						}
						root.ImportRemoteSpans(rec)
					}
					rec := root.GetRecording()
					var seen int
					for idx := range rec {
						rec[idx].Structured(func(any *types.Any) {
							if !types.Is(any, &cs) {
								return
							}
							seen++
							if err := protoutil.Unmarshal(any.Value, &cs); err != nil {
								b.Fatal(err)
							}
						})
					}
					if withStats != seen {
						b.Fatal(seen)
					}
					root.Finish()
				}
			})
		}
	}
}
