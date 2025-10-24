// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicForEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test verifies that topic naming works for various combinations of table/families.
	// Versions prior to 22.1 did not initialize TargetSpecifications, and used Tables instead.
	// Both flavors are tested -- with "old proto" tests testing topic naming with "Tables" field.
	for _, tc := range []struct {
		name      string
		details   jobspb.ChangefeedDetails
		event     cdcevent.Metadata
		expectErr string
		topicName string
	}{
		{
			name: "old proto",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{1: jobspb.ChangefeedTargetTable{StatementTimeName: "t1"}},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "t1"},
			topicName: "t1",
		},
		{
			name: "old proto no such topic",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{
					1: jobspb.ChangefeedTargetTable{StatementTimeName: "t1"},
					2: jobspb.ChangefeedTargetTable{StatementTimeName: "t2"},
				},
			},
			event:     cdcevent.Metadata{TableID: 3},
			expectErr: "no TargetSpecification for row",
		},
		{
			name: "old proto table renamed",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{1: jobspb.ChangefeedTargetTable{StatementTimeName: "old_name"}},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name"},
			topicName: "old_name",
		},
		{
			name: "old proto family ignored",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{1: jobspb.ChangefeedTargetTable{StatementTimeName: "t1"}},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "t1", FamilyID: 2, FamilyName: "fam"},
			topicName: "t1",
		},
		{
			name: "full table",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						TableID:           1,
						StatementTimeName: "t1",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "t1"},
			topicName: "t1",
		},
		{
			name: "full table renamed",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						TableID:           1,
						StatementTimeName: "old_name",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name"},
			topicName: "old_name",
		},
		{
			name: "single family",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						Type:              jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
						TableID:           1,
						FamilyName:        "fam",
						StatementTimeName: "t1",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name", FamilyName: "fam", FamilyID: 1},
			topicName: "t1.fam",
		},
		{
			name: "each family",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
						TableID:           1,
						FamilyName:        "fam",
						StatementTimeName: "old_name",
					},
					{
						Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
						TableID:           1,
						FamilyName:        "fam2",
						StatementTimeName: "old_name",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name", FamilyName: "fam2", FamilyID: 2},
			topicName: "old_name.fam2",
		},
		{
			name: "wrong family",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						Type:              jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
						TableID:           1,
						FamilyName:        "fam",
						StatementTimeName: "t1",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name", FamilyName: "wrong", FamilyID: 0},
			expectErr: "no TargetSpecification for row",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := kvEventToRowConsumer{
				details:              makeChangefeedConfigFromJobDetails(tc.details),
				topicDescriptorCache: make(map[TopicIdentifier]TopicDescriptor),
			}
			tn, err := MakeTopicNamer(AllTargets(tc.details))
			require.NoError(t, err)

			td, err := c.topicForEvent(tc.event)
			if tc.expectErr == "" {
				require.NoError(t, err)
				topicName, err := tn.Name(td)
				require.NoError(t, err)
				require.Equal(t, tc.topicName, topicName)
			} else {
				require.Regexp(t, tc.expectErr, err)
				require.Equal(t, noTopic{}, td)
			}
		})
	}
}

// TestShardingByKey tests that the sharding function is deterministic and
// maps keys within a range of [0, numWorkers).
func TestShardingByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()
	p := parallelEventConsumer{numWorkers: 16, hasher: makeHasher()}

	poolSize := 10000
	eventPool := make([]kvevent.Event, poolSize)
	for i := 0; i < poolSize; i++ {
		eventPool[i] = makeKVEventKeyOnly(rng, 128)
	}

	seen := map[string]int64{}
	for i := 0; i < poolSize; i++ {
		ev := eventPool[i]

		b := p.getBucketForEvent(ev)
		key := getKeyFromKVEvent(ev)

		assert.True(t, 0 <= b && b < 16)

		if bucket, ok := seen[key]; ok {
			assert.Equal(t, bucket, b)
		}
		seen[key] = b
	}
}

func BenchmarkShardingByKey(b *testing.B) {
	rng, _ := randutil.NewTestRand()
	p := parallelEventConsumer{numWorkers: 32, hasher: makeHasher()}

	poolSize := 1000
	eventPool := make([]kvevent.Event, poolSize)
	for i := 0; i < poolSize; i++ {
		eventPool[i] = makeKVEventKeyOnly(rng, 2<<31-1)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.getBucketForEvent(eventPool[rng.Intn(len(eventPool))])
	}
}

func makeKVEventKeyOnly(rng *rand.Rand, upper int) kvevent.Event {
	testTableID := 42

	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(uint32(testTableID)),
		tree.NewDInt(tree.DInt(rng.Intn(upper))),
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	return kvevent.MakeKVEvent(&kvpb.RangeFeedEvent{
		Val: &kvpb.RangeFeedValue{
			Key: key,
		},
	})
}

func getKeyFromKVEvent(ev kvevent.Event) string {
	return ev.KV().Key.String()
}
