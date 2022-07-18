package keyvisualizer

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)


// ┌────────────────────────┐       ┌──────────────────────┐
// │                        │       │                      │
// │   Tenant               │     ┌►│KV Node               │
// │                        │     │ │C8nBoundarySubscriber │
// │                        │     │ │            │         │
// │   ┌─────────────────┐  │     │ │ ┌──────────┼───────┐ │
// │   │ Singleton Job   │  │     │ │ │  Store   │       │ │
// │   │                 │  │     │ │ │          │       │ │
// │   │                 │  │ RPC │ │ │          ▼       │ │
// │   │SpanStatsConsumer├──┼─────┘ │ │  SpanStatsCollector│
// │   │                 │  │       │ │                  │ │
// │   │                 │  │       │ │                  │ │
// │   └─────────────────┘  │       │ └──────────────────┘ │
// │                        │       │                      │
// └────────────────────────┘       └──────────────────────┘


type BoundaryUpdate keyvispb.Boundaries

// SpanStatsCollector manages the collection of store-level batch requests
// per tenant. It opaquely manages the persistence of the last N tenant
// histograms for fault-tolerance purposes.
type SpanStatsCollector interface {
	// SaveBoundaries will stash a tenant's desired collection boundaries to be
	// installed at the beginning of the next sample period.
	SaveBoundaries(boundaries keyvispb.Boundaries) error

	// Increment will increase the counter for a tenant's collection boundary
	// that matches `sp`
	Increment(id roachpb.TenantID, sp roachpb.Span) error

	// GetSamples will return previous samples collected on behalf of a tenant
	// between `start` and `end` inclusive.
	// The histogram for the current sample period will not be returned.
	GetSamples(id roachpb.TenantID, start, end hlc.Timestamp) []keyvispb.Sample
}

// CollectionBoundarySubscriber manages the rangefeed on desired tenant
// collection boundaries.
type CollectionBoundarySubscriber interface {
	Start(ctx context.Context, s *stop.Stopper) error

	// Subscribe installs the function to be called whenever the rangefeed
	// produces a new event.
	Subscribe(func(ctx context.Context, boundaries BoundaryUpdate))
}


// SpanStatsConsumer manages the tenant's periodic polling for,
// and processing of `keyvispb.Sample`.
// It also manages the tenant's desired collection boundaries,
// and communicates this to KV.
type SpanStatsConsumer interface {
	// FetchStats will issue the `GetSamplesFromAllNodes` RPC to a KV node.
	// When it receives the response, it will perform downsampling,
	// data normalization, and ultimately write to the tenant's system table.
	FetchStats(start, end hlc.Timestamp) error

	// DecideBoundaries will issue the `SaveBoundaries` RPC to a KV node.
	// It has the ability to decide 1) if new boundaries should be installed
	// and 2) what those new installed boundaries should be.
	DecideBoundaries()
}
