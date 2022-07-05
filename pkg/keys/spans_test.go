package keys_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRangeSpansOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
	}

	assertOrder := func(spans []roachpb.Span) {
		for i := 1; i < len(spans); i++ {
			require.GreaterOrEqual(t, spans[i].Key.Compare(spans[i-1].EndKey), 0,
				"spans must be ordered and non-overlapping, found %s > %s", spans[i-1].EndKey, spans[i].Key)
		}
	}

	assertOrder(keys.MakeRangeSpans(&desc))
	assertOrder(keys.MakeReplicatedRangeSpans(&desc))
	assertOrder(keys.MakeReplicatedRangeSpansExceptLockTable(&desc))
	assertOrder(keys.MakeReplicatedRangeSpansExceptRangeID(&desc))
}
