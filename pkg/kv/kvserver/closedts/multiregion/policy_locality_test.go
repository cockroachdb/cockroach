package multiregion

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestPolicyLocalityKey(t *testing.T) {
	testCases := []struct {
		name     string
		key      PolicyLocalityKey
		wantIdx  int
		wantBack PolicyLocalityKey // Expected key after round-trip conversion
	}{
		{
			name: "LAG policy with undefined locality",
			key: PolicyLocalityKey{
				Policy:   roachpb.LAG_BY_CLUSTER_SETTING,
				Locality: roachpb.LocalityComparisonType_UNDEFINED,
			},
			wantIdx: 0,
		},
		{
			name: "LEAD policy with undefined locality",
			key: PolicyLocalityKey{
				Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
				Locality: roachpb.LocalityComparisonType_UNDEFINED,
			},
			wantIdx: 1,
		},
		{
			name: "LEAD policy with cross region",
			key: PolicyLocalityKey{
				Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
				Locality: roachpb.LocalityComparisonType_CROSS_REGION,
			},
			wantIdx: numOfPolicies,
		},
		{
			name: "LEAD policy with cross zone",
			key: PolicyLocalityKey{
				Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
				Locality: roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			},
			wantIdx: numOfPolicies + 1,
		},
		{
			name: "LEAD policy with same zone",
			key: PolicyLocalityKey{
				Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
				Locality: roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE,
			},
			wantIdx: numOfPolicies + 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test ToIndex
			idx := tc.key.ToIndex()
			require.Equal(t, tc.wantIdx, idx)

			// Test round-trip conversion
			backKey := EntryIdx(idx).ToPolicyLocalityKey()
			if tc.wantBack.Policy != 0 {
				require.Equal(t, tc.wantBack, backKey)
			} else {
				require.Equal(t, tc.key, backKey)
			}
		})
	}
}

func TestPolicyLocalityToTimestampMap(t *testing.T) {
	makeTS := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	t.Run("basic operations", func(t *testing.T) {
		m := PolicyLocalityToTimestampMap{}

		// Test initial state
		require.Equal(t, numOfEntries, m.Len())
		for i := 0; i < m.Len(); i++ {
			require.Equal(t, hlc.Timestamp{}, m.Timestamps[i])
		}

		// Test Set and Get
		key := PolicyLocalityKey{
			Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
			Locality: roachpb.LocalityComparisonType_CROSS_REGION,
		}
		ts := makeTS(100)
		m.Set(key, ts)
		require.Equal(t, ts, m.Get(key))

		// Test Reset
		m.Reset()
		require.Equal(t, hlc.Timestamp{}, m.Get(key))
	})

	t.Run("all combinations", func(t *testing.T) {
		m := PolicyLocalityToTimestampMap{}
		testData := map[PolicyLocalityKey]hlc.Timestamp{
			{Policy: roachpb.LAG_BY_CLUSTER_SETTING, Locality: roachpb.LocalityComparisonType_UNDEFINED}:             makeTS(1),
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, Locality: roachpb.LocalityComparisonType_UNDEFINED}:              makeTS(2),
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, Locality: roachpb.LocalityComparisonType_CROSS_REGION}:           makeTS(3),
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, Locality: roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE}: makeTS(4),
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, Locality: roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE}:  makeTS(5),
		}

		// Set all combinations
		for key, ts := range testData {
			m.Set(key, ts)
		}

		// Verify all combinations
		for key, wantTS := range testData {
			require.Equal(t, wantTS, m.Get(key))
		}
	})

	t.Run("index bounds", func(t *testing.T) {
		validIndices := []EntryIdx{
			0,                          // First valid index
			EntryIdx(numOfEntries - 1), // Last valid index
		}
		for _, idx := range validIndices {
			require.True(t, idx.valid())
			key := idx.ToPolicyLocalityKey()
			require.True(t, EntryIdx(key.ToIndex()).valid())
		}

		invalidIndices := []EntryIdx{
			-1,                         // Below minimum
			EntryIdx(numOfEntries),     // At maximum
			EntryIdx(numOfEntries + 1), // Above maximum
		}
		for _, idx := range invalidIndices {
			require.False(t, idx.valid())
		}
	})

	t.Run("timestamp updates", func(t *testing.T) {
		m := PolicyLocalityToTimestampMap{}
		key := PolicyLocalityKey{
			Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
			Locality: roachpb.LocalityComparisonType_CROSS_REGION,
		}

		// Test increasing timestamps
		ts1 := makeTS(100)
		ts2 := makeTS(200)
		m.Set(key, ts1)
		require.Equal(t, ts1, m.Get(key))
		m.Set(key, ts2)
		require.Equal(t, ts2, m.Get(key))

		// Test decreasing timestamps
		ts3 := makeTS(50)
		m.Set(key, ts3)
		require.Equal(t, ts3, m.Get(key))
	})
}
