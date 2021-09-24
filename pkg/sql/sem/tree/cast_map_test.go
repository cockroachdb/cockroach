package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
)

// TestCastMap tests that every cast in tree.castMap can be performed by
// PerformCast.
func TestCastMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	rng, _ := randutil.NewTestPseudoRand()
	evalCtx.Planner = &faketreeeval.DummyEvalPlanner{}

	tree.ForEachCast(func(src, tgt oid.Oid) {
		srcType := types.OidToType[src]
		tgtType := types.OidToType[tgt]
		srcDatum := randgen.RandDatum(rng, srcType, false /* nullOk */)

		// TODO(mgartner): We do not allow casting a negative integer to bit
		// types with unbounded widths. Until we add support for this, we
		// ensure that the srcDatum is positive.
		if srcType.Family() == types.IntFamily && tgtType.Family() == types.BitFamily {
			srcVal := *srcDatum.(*tree.DInt)
			if srcVal < 0 {
				srcDatum = tree.NewDInt(-srcVal)
			}
		}

		_, err := tree.PerformCast(&evalCtx, srcDatum, tgtType)
		if err != nil && pgerror.HasCandidateCode(err) && pgerror.GetPGCode(err) == pgcode.CannotCoerce {
			t.Errorf("cast from %s to %s failed: %s", srcType, tgtType, err)
		}
	})
}
