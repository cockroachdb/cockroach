// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// Option configures a RangeFeed.
type Option interface {
	set(*config)
}

type config struct {
	scanConfig
	retryOptions       retry.Options
	onInitialScanDone  OnInitialScanDone
	withInitialScan    bool
	onInitialScanError OnInitialScanError
	// useRowTimestampInInitialScan indicates that when rows are scanned in an
	// initial scan, they should report their timestamp as it exists in KV as
	// opposed to the timestamp at which the scan occurred. Both behaviors can
	// be sane depending on your use case.
	useRowTimestampInInitialScan bool

	invoker func(func() error) error

	withDiff              bool
	withFiltering         bool
	withMatchingOriginIDs []uint32
	consumerID            int64
	onUnrecoverableError  OnUnrecoverableError
	onCheckpoint          OnCheckpoint
	frontierQuantize      time.Duration
	onFrontierAdvance     OnFrontierAdvance
	frontierVisitor       FrontierSpanVisitor
	onSSTable             OnSSTable
	onValues              OnValues
	onDeleteRange         OnDeleteRange
	onMetadata            OnMetadata
	extraPProfLabels      []string
}

type scanConfig struct {
	// scanParallelism controls the number of concurrent scan requests
	// that can be issued.  If unspecified, only 1 scan request at a time is issued.
	scanParallelism func() int

	// targetScanBytes requests that many bytes to be returned per scan request.
	// adjusting this setting should almost always be used together with the setting
	// to configure memory monitor.
	targetScanBytes int64

	// mon is the memory monitor to while scanning.
	mon *mon.BytesMonitor

	// OnSpanDone is invoked when initial scan of some span is completed.
	OnSpanDone OnScanCompleted

	// overSystemTable indicates whether this rangefeed is over a system table
	// (used internally for CRDB's own functioning) and therefore should be
	// treated with a more appropriate admission pri (NormalPri instead of
	// BulkNormalPri).
	overSystemTable bool
}

type optionFunc func(*config)

func (o optionFunc) set(c *config) { o(c) }

// OnInitialScanDone is called when an initial scan is finished before any rows
// from the rangefeed are supplied.
type OnInitialScanDone func(ctx context.Context)

// OnInitialScanError is called when an initial scan encounters an error. It
// allows the caller to tell the RangeFeed to stop as opposed to retrying
// endlessly.
type OnInitialScanError func(ctx context.Context, err error) (shouldFail bool)

// OnUnrecoverableError is called when the rangefeed exits with an unrecoverable
// error (preventing internal retries). One example is when the rangefeed falls
// behind to a point where the frontier timestamp precedes the GC threshold, and
// thus will never work. The callback lets callers find out about such errors
// (possibly, in our example, to start a new rangefeed with an initial scan).
type OnUnrecoverableError func(ctx context.Context, err error)

// WithInitialScan enables an initial scan of the data in the span. The rows of
// an initial scan will be passed to the value function used to construct the
// RangeFeed. Upon completion of the initial scan, the passed function (if
// non-nil) will be called. The initial scan may be restarted and thus rows
// may be observed multiple times. The caller cannot rely on rows being returned
// in order.
func WithInitialScan(f OnInitialScanDone) Option {
	return optionFunc(func(c *config) {
		c.withInitialScan = true
		c.onInitialScanDone = f
	})
}

// WithOnInitialScanError sets up a callback to report errors during the initial
// scan to the caller. The caller may instruct the RangeFeed to halt rather than
// retrying endlessly. This option will not enable an initial scan; it must be
// used in conjunction with WithInitialScan to have any effect.
func WithOnInitialScanError(f OnInitialScanError) Option {
	return optionFunc(func(c *config) {
		c.onInitialScanError = f
	})
}

// WithRowTimestampInInitialScan indicates whether the timestamp of rows
// reported during an initial scan should correspond to the timestamp of that
// row as it exists in KV or should correspond to the timestamp of the initial
// scan. The default is false, indicating that the timestamp should correspond
// to the timestamp of the initial scan.
func WithRowTimestampInInitialScan(shouldUse bool) Option {
	return optionFunc(func(c *config) {
		c.useRowTimestampInInitialScan = shouldUse
	})
}

// WithOnInternalError sets up a callback to report unrecoverable errors during
// operation.
func WithOnInternalError(f OnUnrecoverableError) Option {
	return optionFunc(func(c *config) {
		c.onUnrecoverableError = f
	})
}

// WithDiff makes an option to set whether rangefeed events carry the previous
// value in addition to the new value. The option defaults to false. If set,
// initial scan events will carry the same value for both Value and PrevValue.
func WithDiff(withDiff bool) Option {
	return optionFunc(func(c *config) {
		c.withDiff = withDiff
	})
}

// WithFiltering makes an option to set whether to filter out rangefeeds events
// where the user has set omit_from_changefeeds in their session.
func WithFiltering(withFiltering bool) Option {
	return optionFunc(func(c *config) {
		c.withFiltering = withFiltering
	})
}

func WithOriginIDsMatching(originIDs ...uint32) Option {
	return optionFunc(func(c *config) {
		c.withMatchingOriginIDs = originIDs
	})
}

func WithConsumerID(cid int64) Option {
	return optionFunc(func(c *config) {
		c.consumerID = cid
	})
}

// WithInvoker makes an option to invoke the rangefeed tasks such as running the
// the client and processing events emitted by the client with a caller-supplied
// function, which can make it easier to introspect into work done by a given
// caller as the stacks for these tasks will now include the caller's invoker.
func WithInvoker(invoker func(func() error) error) Option {
	return optionFunc(func(c *config) {
		c.invoker = invoker
	})
}

// WithRetry configures the retry options for the rangefeed.
func WithRetry(options retry.Options) Option {
	return optionFunc(func(c *config) {
		c.retryOptions = options
	})
}

// WithOnValues sets up a callback that's invoked whenever a batch of values is
// passed such as during initial scans, allowing passing it as a batch to the
// client rather than key-by-key to reduce overhead. This however comes with
// some limitations: for batches passed this way the rangefeed client will not
// process individual values and instead leaves this to the caller, meaning that
// the options WithRowTimestampInInitialScan is implied, and WithDiff is ignored
// as these are per-key processing that is not performed on batches.
func WithOnValues(fn OnValues) Option {
	return optionFunc(func(c *config) {
		c.onValues = fn
	})
}

// OnCheckpoint is called when a rangefeed checkpoint occurs.
type OnCheckpoint func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint)

// WithOnCheckpoint sets up a callback that's invoked whenever a check point
// event is emitted.
func WithOnCheckpoint(f OnCheckpoint) Option {
	return optionFunc(func(c *config) {
		c.onCheckpoint = f
	})
}

// OnSSTable is called when an SSTable is ingested. If this callback is not
// provided, a catchup scan will be run instead that will include the contents
// of these SSTs.
//
// 'registeredSpan' is a span of rangefeed registration that emits the SSTable.
//
// Note that the SST is emitted as it was ingested, so it may contain keys
// outside of the rangefeed span, and the caller should prune the SST contents
// as appropriate. Futhermore, these events do not contain previous values as
// requested by WithDiff, and callers must obtain these themselves if needed.
//
// Also note that AddSSTable requests that do not set the
// SSTTimestampToRequestTimestamp param, possibly writing below the closed
// timestamp, will cause affected rangefeeds to be disconnected with a terminal
// MVCCHistoryMutationError and thus will not be emitted here -- there should be
// no such requests into spans with rangefeeds across them, but it is up to
// callers to ensure this.
type OnSSTable func(
	ctx context.Context,
	sst *kvpb.RangeFeedSSTable,
	registeredSpan roachpb.Span,
)

// WithOnSSTable sets up a callback that's invoked whenever an SSTable is
// ingested.
func WithOnSSTable(f OnSSTable) Option {
	return optionFunc(func(c *config) {
		c.onSSTable = f
	})
}

// OnMetadata is called when a RangefeedMetadata event is passed to the eventCh.
// This occurs when a partial rangefeed begins, and if the the rangefeed client
// was initialized with an OnMetadata function.
type OnMetadata func(ctx context.Context, value *kvpb.RangeFeedMetadata)

// WithOnMetadata sets up a callback that's invoked when a partial rangefeed is
// spawned.
func WithOnMetadata(fn OnMetadata) Option {
	return optionFunc(func(c *config) {
		c.onMetadata = fn
	})
}

// OnDeleteRange is called when an MVCC range tombstone is written (e.g. when
// DeleteRange is called with UseRangeTombstone, but not when the range is
// deleted using point tombstones). If this callback is not provided, an error
// is emitted when these are encountered.
type OnDeleteRange func(ctx context.Context, value *kvpb.RangeFeedDeleteRange)

// WithOnDeleteRange sets up a callback that's invoked whenever an MVCC range
// deletion tombstone is written.
func WithOnDeleteRange(f OnDeleteRange) Option {
	return optionFunc(func(c *config) {
		c.onDeleteRange = f
	})
}

// OnFrontierAdvance is called when the rangefeed frontier is advanced with the
// new frontier timestamp.
type OnFrontierAdvance func(ctx context.Context, timestamp hlc.Timestamp)

// WithOnFrontierAdvance sets up a callback that's invoked whenever the
// rangefeed frontier is advanced.
func WithOnFrontierAdvance(f OnFrontierAdvance) Option {
	return optionFunc(func(c *config) {
		c.onFrontierAdvance = f
	})
}

// VisitableFrontier is the subset of the span.Frontier interface required to
// inspect the content of the frontier.
type VisitableFrontier interface {
	Entries(span.Operation)
}

// FrontierSpanVisitor is called when the frontier is updated by a checkpoint,
// and is passed the iterable frontier, as well as if the checkpoint advanced it
// when it was added.
type FrontierSpanVisitor func(ctx context.Context, advanced bool, frontier VisitableFrontier)

// WithFrontierSpanVisitor sets up a callback to optionally inspect the frontier
// after a checkpoint is processed.
func WithFrontierSpanVisitor(fn FrontierSpanVisitor) Option {
	return optionFunc(func(c *config) {
		c.frontierVisitor = fn
	})
}

func initConfig(c *config, options []Option) {
	*c = config{} // the default config is its zero value
	for _, o := range options {
		o.set(c)
	}

	if c.targetScanBytes == 0 {
		c.targetScanBytes = 1 << 19 // 512 KiB
	}
}

// WithInitialScanParallelismFn configures rangefeed to issue up to specified number
// of concurrent initial scan requests.
func WithInitialScanParallelismFn(parallelismFn func() int) Option {
	return optionFunc(func(c *config) {
		c.scanParallelism = parallelismFn
	})
}

// WithTargetScanBytes configures rangefeed to request specified number of bytes per scan request.
// This option should be used together with the option to configure memory monitor.
func WithTargetScanBytes(target int64) Option {
	return optionFunc(func(c *config) {
		c.targetScanBytes = target
	})
}

// WithMemoryMonitor configures rangefeed to use memory monitor when issuing scan requests.
func WithMemoryMonitor(mon *mon.BytesMonitor) Option {
	return optionFunc(func(c *config) {
		c.mon = mon
	})
}

// OnScanCompleted is called when the rangefeed initial scan completes scanning
// the span.  An error returned from this function is handled based on the WithOnInitialScanError
// option.  If the error handler is not set, the scan is retried based on the
// WithSAcanRetryBehavior option.
type OnScanCompleted func(ctx context.Context, sp roachpb.Span) error

// WithOnScanCompleted sets up a callback which is invoked when a span (or part of the span)
// have been completed when performing an initial scan.
func WithOnScanCompleted(fn OnScanCompleted) Option {
	return optionFunc(func(c *config) {
		c.OnSpanDone = fn
	})
}

// WithPProfLabel configures rangefeed to annotate go routines started by range feed
// with the specified key=value label.
func WithPProfLabel(key, value string) Option {
	return optionFunc(func(c *config) {
		c.extraPProfLabels = append(c.extraPProfLabels, key, value)
	})
}

// WithSystemTablePriority communicates that the rangefeed is over a system
// table and thus operates at a higher priority (this primarily affects
// admission control).
func WithSystemTablePriority() Option {
	return optionFunc(func(c *config) {
		c.overSystemTable = true
	})
}

// WithFrontierQuantized enables quantization of timestamps down to the nearest
// multiple of d to potentially reduce overhead in the frontier thanks to more
// sub-spans having equal timestamps and thus being able to be merged, resulting
// in fewer distinct spans in the frontier.
func WithFrontierQuantized(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.frontierQuantize = d
	})
}
