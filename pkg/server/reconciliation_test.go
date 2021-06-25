package server

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSpanConfigUpdateSeenByAllStores(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	var storeSpecs []base.StoreSpec
	for i := 0; i < numStores; i++ {
		storeSpecs = append(storeSpecs, base.StoreSpec{InMemory: true})
	}

	// TODO(zcfgs-pod): Plumb in testing knob to disable the automatic
	// reconciliation job.
	// TODO(zcfgs-pod): Plumb in testing knob to filter out span config updates
	// -- here we're only interested in making sure that all stores receive the
	// update.

	id := rand.Uint32()
	var seenCount int32 = 0
	var once sync.Once
	waitCh := make(chan struct{})
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: storeSpecs,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				SpanConfigUpdateInterceptor: func(update spanconfig.Update) {
					if update.Entry.Config.NumReplicas == int32(id) {
						if atomic.AddInt32(&seenCount, 1) == numStores {
							once.Do(func() { close(waitCh) })
						}
					}
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	nameSpaceTableStart := s.ExecutorConfig().(sql.ExecutorConfig).Codec.TablePrefix(keys.NamespaceTableID)
	nameSpaceTableSpan := roachpb.Span{
		Key:    nameSpaceTableStart,
		EndKey: nameSpaceTableStart.PrefixEnd(),
	}

	var update []roachpb.SpanConfigEntry
	var delete []roachpb.Span
	update = append(update, roachpb.SpanConfigEntry{
		Span: nameSpaceTableSpan,
		Config: roachpb.SpanConfig{
			NumReplicas: int32(id),
		},
	})

	log.Infof(ctx, "xxx: test updating some random span entry: %s (id=%d)", nameSpaceTableSpan, id)
	require.NoError(t, s.Node().(*Node).UpdateSpanConfigEntries(ctx, update, delete))

	select {
	case <-waitCh:
	case <-time.After(10 * time.Second):
		t.Fatal(`test timed out`)
	}
}
