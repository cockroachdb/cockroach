package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

type registration interface {
	publish(ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation)
	disconnect(pErr *kvpb.Error)
	runOutputLoop(ctx context.Context, forStacks roachpb.RangeID)
	drainAllocations(ctx context.Context)
	waitForCaughtUp(ctx context.Context) error
	setID(int64)
	setSpanAsKeys()
	getSpan() roachpb.Span
	getCatchUpTimestamp() hlc.Timestamp
	getWithFiltering() bool
	getWithOmitRemote() bool
	Range() interval.Range
	ID() uintptr
	setDisconnected() (needCleanUp bool)
	getUnreg() func()
	getWithDiff() bool
}

var _ registration = &bufferedRegistration{}
