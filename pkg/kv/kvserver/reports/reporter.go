// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reports

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// ReporterInterval is the interval between two generations of the reports.
// When set to zero - disables the report generation.
var ReporterInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.replication_reports.interval",
	"the frequency for generating the replication_constraint_stats, replication_stats_report and "+
		"replication_critical_localities reports (set to 0 to disable)",
	time.Minute,
	settings.NonNegativeDuration,
	settings.WithPublic)

// Reporter periodically produces a couple of reports on the cluster's data
// distribution: the system tables: replication_constraint_stats,
// replication_stats_report and replication_critical_localities.
//
// TODO(irfansharif): After #67679 these replication reports will be the last
// remaining use of the system config span in KV. Strawman: we could hoist all
// this code above KV and run it for each tenant. We'd have to expose a view
// into node liveness and store descriptors, and instead of using the system
// config span we could consult the tenant-scoped system.zones directly.
type Reporter struct {
	// Contains the list of the stores of the current node
	localStores *kvserver.Stores
	// The store that is the current meta 1 leaseholder
	meta1LeaseHolder *kvserver.Store
	// Latest zone config
	latestConfig *config.SystemConfig

	db        *kv.DB
	liveness  *liveness.NodeLiveness
	settings  *cluster.Settings
	storePool *storepool.StorePool
	executor  isql.Executor
	cfgs      config.SystemConfigProvider

	frequencyMu struct {
		syncutil.Mutex
		interval time.Duration
		changeCh chan struct{}
	}
}

// NewReporter creates a Reporter.
func NewReporter(
	db *kv.DB,
	localStores *kvserver.Stores,
	storePool *storepool.StorePool,
	st *cluster.Settings,
	liveness *liveness.NodeLiveness,
	executor isql.Executor,
	provider config.SystemConfigProvider,
) *Reporter {
	r := Reporter{
		db:          db,
		localStores: localStores,
		storePool:   storePool,
		settings:    st,
		liveness:    liveness,
		executor:    executor,
		cfgs:        provider,
	}
	r.frequencyMu.changeCh = make(chan struct{})
	return &r
}

// reportInterval returns the current value of the frequency setting and a
// channel that will get closed when the value is not current any more.
func (stats *Reporter) reportInterval() (time.Duration, <-chan struct{}) {
	stats.frequencyMu.Lock()
	defer stats.frequencyMu.Unlock()
	return ReporterInterval.Get(&stats.settings.SV), stats.frequencyMu.changeCh
}

// Start the periodic calls to Update().
func (stats *Reporter) Start(ctx context.Context, stopper *stop.Stopper) {
	ReporterInterval.SetOnChange(&stats.settings.SV, func(ctx context.Context) {
		stats.frequencyMu.Lock()
		defer stats.frequencyMu.Unlock()
		// Signal the current waiter (if any), and prepare the channel for future
		// ones.
		ch := stats.frequencyMu.changeCh
		close(ch)
		stats.frequencyMu.changeCh = make(chan struct{})
		stats.frequencyMu.interval = ReporterInterval.Get(&stats.settings.SV)
	})
	_ = stopper.RunAsyncTask(ctx, "stats-reporter", func(ctx context.Context) {
		ctx = logtags.AddTag(ctx, "replication-reporter", nil /* value */)
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		var timer timeutil.Timer
		defer timer.Stop()

		replStatsSaver := makeReplicationStatsReportSaver()
		constraintsSaver := makeReplicationConstraintStatusReportSaver()
		criticalLocSaver := makeReplicationCriticalLocalitiesReportSaver()

		for {
			// Read the interval setting. We'll generate a report and then sleep for
			// that long. We'll also wake up if the setting changes; that's useful for
			// tests which want to lower the setting drastically and expect the report
			// to be regenerated quickly, and also for users increasing the frequency.
			interval, changeCh := stats.reportInterval()

			var timerCh <-chan time.Time
			if interval != 0 {
				// If (some store on) this node is the leaseholder for range 1, do the
				// work.
				stats.meta1LeaseHolder = stats.meta1LeaseHolderStore(ctx)
				if stats.meta1LeaseHolder != nil {
					if err := stats.update(
						ctx, &constraintsSaver, &replStatsSaver, &criticalLocSaver,
					); err != nil {
						log.Errorf(ctx, "failed to generate replication reports: %s", err)
					}
				}
				timer.Reset(interval)
				timerCh = timer.C
			}

			// Wait until the timer expires (if there's a timer) or until there's an
			// update to the frequency setting.
			select {
			case <-timerCh:
				timer.Read = true
			case <-changeCh:
			case <-ctx.Done():
				return
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// update regenerates all the reports and saves them using the provided savers.
func (stats *Reporter) update(
	ctx context.Context,
	constraintsSaver *replicationConstraintStatsReportSaver,
	replStatsSaver *replicationStatsReportSaver,
	locSaver *replicationCriticalLocalitiesReportSaver,
) error {
	start := timeutil.Now()
	log.VEventf(ctx, 2, "updating replication reports...")
	defer func() {
		log.VEventf(ctx, 2, "updating replication reports... done. Generation took: %s.",
			timeutil.Since(start))
	}()
	stats.updateLatestConfig()
	if stats.latestConfig == nil {
		return nil
	}

	allStores := stats.storePool.GetStores()
	var storesFromGossip StoreResolver = func(
		id roachpb.StoreID,
	) roachpb.StoreDescriptor {
		// We'll return empty descriptors for stores that gossip doesn't have a
		// descriptor for. These stores will be considered to satisfy all
		// constraints.
		// TODO(andrei): note down that some descriptors were missing from gossip
		// somewhere in the report.
		return allStores[id]
	}

	isNodeLive := func(nodeID roachpb.NodeID) bool {
		return stats.liveness.GetNodeVitalityFromCache(nodeID).IsLive(livenesspb.Metrics)
	}

	nodeLocalities := make(map[roachpb.NodeID]roachpb.Locality, len(allStores))
	for _, storeDesc := range allStores {
		nodeDesc := storeDesc.Node
		// Note: We might overwrite the node's localities here. We assume that all
		// the stores for a node have the same node descriptor.
		nodeLocalities[nodeDesc.NodeID] = nodeDesc.Locality
	}

	// Create the visitors that we're going to pass to visitRanges() below.
	constraintConfVisitor := makeConstraintConformanceVisitor(
		ctx, stats.latestConfig, storesFromGossip)
	localityStatsVisitor := makeCriticalLocalitiesVisitor(
		ctx, nodeLocalities, stats.latestConfig,
		storesFromGossip, isNodeLive)
	replicationStatsVisitor := makeReplicationStatsVisitor(ctx, stats.latestConfig, isNodeLive)

	// Iterate through all the ranges.
	const descriptorReadBatchSize = 10000
	rangeIter := makeMeta2RangeIter(stats.db, descriptorReadBatchSize)
	if err := visitRanges(
		ctx, &rangeIter, stats.latestConfig,
		&constraintConfVisitor, &localityStatsVisitor, &replicationStatsVisitor,
	); err != nil {
		if errors.HasType(err, (*visitorError)(nil)) {
			log.Errorf(ctx, "some reports have not been generated: %s", err)
		} else {
			return errors.Wrap(err, "failed to compute constraint conformance report")
		}
	}

	if !constraintConfVisitor.failed() {
		if err := constraintsSaver.Save(
			ctx, constraintConfVisitor.report, timeutil.Now() /* reportTS */, stats.db, stats.executor,
		); err != nil {
			return errors.Wrap(err, "failed to save constraint report")
		}
	}
	if !localityStatsVisitor.failed() {
		if err := locSaver.Save(
			ctx, localityStatsVisitor.Report(), timeutil.Now() /* reportTS */, stats.db, stats.executor,
		); err != nil {
			return errors.Wrap(err, "failed to save locality report")
		}
	}
	if !replicationStatsVisitor.failed() {
		if err := replStatsSaver.Save(
			ctx, replicationStatsVisitor.Report(),
			timeutil.Now() /* reportTS */, stats.db, stats.executor,
		); err != nil {
			return errors.Wrap(err, "failed to save range status report")
		}
	}
	return nil
}

// meta1LeaseHolderStore returns the node store that is the leaseholder of Meta1
// range or nil if none of the node's stores are holding the Meta1 lease.
func (stats *Reporter) meta1LeaseHolderStore(ctx context.Context) *kvserver.Store {
	const meta1RangeID = roachpb.RangeID(1)
	repl, store, err := stats.localStores.GetReplicaForRangeID(ctx, meta1RangeID)
	if kvpb.IsRangeNotFoundError(err) {
		return nil
	}
	if err != nil {
		log.Fatalf(ctx, "unexpected error when visiting stores: %s", err)
	}
	if repl.OwnsValidLease(ctx, store.Clock().NowAsClockTimestamp()) {
		return store
	}
	return nil
}

func (stats *Reporter) updateLatestConfig() {
	stats.latestConfig = stats.cfgs.GetSystemConfig()
}

// nodeChecker checks whether a node is to be considered alive or not.
type nodeChecker func(nodeID roachpb.NodeID) bool

// zoneResolver resolves ranges to their zone configs. It is optimized for the
// case where a range falls in the same range as a the previously-resolved range
// (which is the common case when asked to resolve ranges in key order).
type zoneResolver struct {
	init bool
	// curObjectID is the object (i.e. usually table) of the configured range.
	curObjectID config.ObjectID
	// curRootZone is the lowest zone convering the previously resolved range
	// that's not a subzone.
	// This is used to compute the subzone for a range.
	curRootZone *zonepb.ZoneConfig
	// curZoneKey is the zone key for the previously resolved range.
	curZoneKey ZoneKey
}

// resolveRange resolves a range to its zone.
func (c *zoneResolver) resolveRange(
	ctx context.Context, rng *roachpb.RangeDescriptor, cfg *config.SystemConfig,
) (ZoneKey, error) {
	if c.checkSameZone(ctx, rng) {
		return c.curZoneKey, nil
	}
	return c.updateZone(ctx, rng, cfg)
}

// setZone remembers the passed-in info as the reference for further
// checkSameZone() calls.
// Clients should generally use the higher-level updateZone().
func (c *zoneResolver) setZone(objectID config.ObjectID, key ZoneKey, rootZone *zonepb.ZoneConfig) {
	c.init = true
	c.curObjectID = objectID
	c.curRootZone = rootZone
	c.curZoneKey = key
}

// updateZone updates the state of the zoneChecker to the zone of the passed-in
// range descriptor.
func (c *zoneResolver) updateZone(
	ctx context.Context, rd *roachpb.RangeDescriptor, cfg *config.SystemConfig,
) (ZoneKey, error) {
	objectID, _ := config.DecodeKeyIntoZoneIDAndSuffix(keys.SystemSQLCodec, rd.StartKey)
	first := true
	var zoneKey ZoneKey
	var rootZone *zonepb.ZoneConfig
	// We're going to walk the zone hierarchy looking for two things:
	// 1) The lowest zone containing rd. We'll use the subzone ID for it.
	// 2) The lowest zone containing rd that's not a subzone.
	// visitZones() walks the zone hierarchy from the bottom upwards.
	found, err := visitZones(
		ctx, rd, cfg, includeSubzonePlaceholders,
		func(_ context.Context, zone *zonepb.ZoneConfig, key ZoneKey) bool {
			if first {
				first = false
				zoneKey = key
			}
			if key.SubzoneID == NoSubzone {
				rootZone = zone
				return true
			}
			return false
		})
	if err != nil {
		return ZoneKey{}, err
	}
	if !found {
		return ZoneKey{}, errors.AssertionFailedf("failed to resolve zone for range: %s", rd)
	}
	c.setZone(objectID, zoneKey, rootZone)
	return zoneKey, nil
}

// checkSameZone returns true if the most specific zone that contains rng is the
// one previously passed to setZone().
//
// NB: This method allows for false negatives (but no false positives). For
// example, if the zoneChecker was previously configured for a range starting at
// /Table/51 and is now queried for /Table/52, it will say that the zones don't
// match even if in fact they do (because neither table defines its own zone
// and they're both inheriting a higher zone).
func (c *zoneResolver) checkSameZone(ctx context.Context, rng *roachpb.RangeDescriptor) bool {
	if !c.init {
		return false
	}

	objectID, keySuffix := config.DecodeKeyIntoZoneIDAndSuffix(keys.SystemSQLCodec, rng.StartKey)
	if objectID != c.curObjectID {
		return false
	}
	_, subzoneIdx := c.curRootZone.GetSubzoneForKeySuffix(keySuffix)
	return subzoneIdx == c.curZoneKey.SubzoneID.ToSubzoneIndex()
}

type visitOpt bool

const (
	ignoreSubzonePlaceholders  visitOpt = false
	includeSubzonePlaceholders visitOpt = true
)

// visitZones applies a visitor to the hierarchy of zone configs that apply to
// the given range, starting from the most specific to the default zone config.
//
// visitor is called for each zone config until it returns true, or until the
// default zone config is reached. It's passed zone configs and the
// corresponding zoneKeys.
//
// visitZones returns true if the visitor returned true and returns false is the
// zone hierarchy was exhausted.
func visitZones(
	ctx context.Context,
	rng *roachpb.RangeDescriptor,
	cfg *config.SystemConfig,
	opt visitOpt,
	visitor func(context.Context, *zonepb.ZoneConfig, ZoneKey) bool,
) (bool, error) {
	id, keySuffix := config.DecodeKeyIntoZoneIDAndSuffix(keys.SystemSQLCodec, rng.StartKey)
	zone, err := getZoneByID(id, cfg)
	if err != nil {
		return false, err
	}

	// We've got the zone config (without considering for inheritance) for the
	// "object" indicated by out key. Now we need to find where the constraints
	// come from. We'll first look downwards - in subzones (if any). If there's no
	// constraints there, we'll look in the zone config that we got. If not,
	// we'll look upwards (e.g. database zone config, default zone config).

	if zone != nil {
		// Try subzones.
		subzone, subzoneIdx := zone.GetSubzoneForKeySuffix(keySuffix)
		if subzone != nil {
			if visitor(ctx, &subzone.Config, MakeZoneKey(id, base.SubzoneIDFromIndex(int(subzoneIdx)))) {
				return true, nil
			}
		}
		// Try the zone for our object.
		if (opt == includeSubzonePlaceholders) || !zone.IsSubzonePlaceholder() {
			if visitor(ctx, zone, MakeZoneKey(id, 0)) {
				return true, nil
			}
		}
	}

	// Go upwards.
	return visitAncestors(ctx, id, cfg, visitor)
}

// visitAncestors invokes the visitor of all the ancestors of the zone
// corresponding to id. The zone corresponding to id itself is not visited.
func visitAncestors(
	ctx context.Context,
	id config.ObjectID,
	cfg *config.SystemConfig,
	visitor func(context.Context, *zonepb.ZoneConfig, ZoneKey) bool,
) (bool, error) {
	// This is a bug: see https://github.com/cockroachdb/cockroach/issues/48123.
	var FIXMEIDONTKNOWWHICHCODECTOUSE = keys.MakeSQLCodec(roachpb.SystemTenantID)

	// Check to see if it's a table. If so, inherit from the database.
	// For all other cases, inherit from the default.
	descVal := cfg.GetValue(catalogkeys.MakeDescMetadataKey(FIXMEIDONTKNOWWHICHCODECTOUSE, descpb.ID(id)))
	if descVal == nil {
		// Couldn't find a descriptor. This is not expected to happen.
		// Let's just look at the default zone config.
		return visitDefaultZone(ctx, cfg, visitor)
	}

	// TODO(ajwerner): Reconsider how this zone config picking apart happens. This
	// isn't how we want to be retreiving table descriptors in general.
	b, err := descbuilder.FromSerializedValue(descVal)
	if err != nil {
		return false, err
	}
	// If it's a database, the parent is the default zone.
	if b == nil || b.DescriptorType() != catalog.Table {
		return visitDefaultZone(ctx, cfg, visitor)
	}
	tableDesc := b.BuildImmutable()
	// If it's a table, the parent is a database.
	zone, err := getZoneByID(config.ObjectID(tableDesc.GetParentID()), cfg)
	if err != nil {
		return false, err
	}
	if zone != nil {
		if visitor(ctx, zone, MakeZoneKey(config.ObjectID(tableDesc.GetParentID()), NoSubzone)) {
			return true, nil
		}
	}
	// The parent database did not have constraints. Its parent is the default zone.
	return visitDefaultZone(ctx, cfg, visitor)
}

func visitDefaultZone(
	ctx context.Context,
	cfg *config.SystemConfig,
	visitor func(context.Context, *zonepb.ZoneConfig, ZoneKey) bool,
) (bool, error) {
	zone, err := getZoneByID(keys.RootNamespaceID, cfg)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get default zone config from: %v", cfg)
	}
	if zone == nil {
		return false, errors.AssertionFailedf("default zone config missing unexpectedly from: %v", cfg)
	}
	return visitor(ctx, zone, MakeZoneKey(keys.RootNamespaceID, NoSubzone)), nil
}

// getZoneByID returns a zone given its id. Inheritance does not apply.
func getZoneByID(id config.ObjectID, cfg *config.SystemConfig) (*zonepb.ZoneConfig, error) {
	zoneVal := cfg.GetValue(config.MakeZoneKey(keys.SystemSQLCodec, descpb.ID(id)))
	if zoneVal == nil {
		return nil, nil
	}
	zone := new(zonepb.ZoneConfig)
	if err := zoneVal.GetProto(zone); err != nil {
		return nil, err
	}
	return zone, nil
}

// StoreResolver is a function resolving a store descriptor by its id. Empty
// store descriptors are to be returned when there's no information available
// for the store.
type StoreResolver func(roachpb.StoreID) roachpb.StoreDescriptor

// rangeVisitor abstracts the interface for range iteration implemented by all
// report generators.
type rangeVisitor interface {
	// visitNewZone/visitSameZone is called by visitRanges() for each range, in
	// order. The visitor will update its report with the range's info. If an
	// error is returned, visit() will not be called anymore before reset().
	// If an error() is returned, failed() needs to return true until reset() is
	// called.
	//
	// Once visitNewZone() has been called once, visitSameZone() is called for
	// further ranges as long as these ranges are covered by the same zone config.
	// As soon as the range is not covered by it, visitNewZone() is called again.
	// The idea is that visitors can maintain state about that zone that applies
	// to multiple ranges, and so visitSameZone() allows them to efficiently reuse
	// that state (in particular, not unmarshall ZoneConfigs again).
	visitNewZone(context.Context, *roachpb.RangeDescriptor) error
	visitSameZone(context.Context, *roachpb.RangeDescriptor)

	// failed returns true if an error was encountered by the last visit() call
	// (and reset( ) wasn't called since).
	// The idea is that, if failed() returns true, the report that the visitor
	// produces will be considered incomplete and not persisted.
	failed() bool
}

// visitorError is returned by visitRanges when one or more visitors failed.
type visitorError struct {
	errs []error
}

func (e *visitorError) Error() string {
	s := make([]string, len(e.errs))
	for i, err := range e.errs {
		s[i] = fmt.Sprintf("%d: %s", i, err)
	}
	return fmt.Sprintf("%d visitors encountered errors:\n%s", len(e.errs), strings.Join(s, "\n"))
}

// visitRanges iterates through all the range descriptors in Meta2 and calls the
// supplied visitors.
//
// An error is returned if some descriptors could not be read. Additionally,
// visitorError is returned if some visitors failed during the iteration. In
// that case, it is expected that the reports produced by those specific
// visitors will not be persisted, but the other reports will.
func visitRanges(
	ctx context.Context, rangeStore RangeIterator, cfg *config.SystemConfig, visitors ...rangeVisitor,
) error {
	origVisitors := make([]rangeVisitor, len(visitors))
	copy(origVisitors, visitors)
	var visitorErrs []error
	var resolver zoneResolver

	var key ZoneKey
	first := true

	// Iterate over all the ranges.
	for {
		// Check for context cancellation.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Grab the next range.
		rd, err := rangeStore.Next(ctx)
		if err != nil {
			return err
		}
		if rd.RangeID == 0 {
			// We're done.
			break
		}

		newKey, err := resolver.resolveRange(ctx, &rd, cfg)
		if err != nil {
			return err
		}
		sameZoneAsPrevRange := !first && key == newKey
		key = newKey
		first = false

		for i := 0; i < len(visitors); {
			v := visitors[i]
			if sameZoneAsPrevRange {
				v.visitSameZone(ctx, &rd)
			} else {
				err = v.visitNewZone(ctx, &rd)
			}

			if err != nil {
				// Sanity check - v.failed() should return an error now (the same as err above).
				if !v.failed() {
					return errors.NewAssertionErrorWithWrappedErrf(err, "expected visitor %T to have failed() after error", v)
				}
				// Remove this visitor; it shouldn't be called any more.
				visitors = append(visitors[:i], visitors[i+1:]...)
				visitorErrs = append(visitorErrs, err)
			} else {
				i++
			}
		}
	}
	if len(visitorErrs) > 0 {
		return &visitorError{errs: visitorErrs}
	}
	return nil
}

// RangeIterator abstracts the interface for reading range descriptors.
type RangeIterator interface {
	// Next returns the next range descriptors (in key order).
	// Returns an empty RangeDescriptor when all the ranges have been exhausted. In that case,
	// the iterator is not to be used any more (except for calling Close(), which will be a no-op).
	//
	// In case of an error, the iterator is automatically closed.
	// It can't be used any more (except for calling Close(), which will be a noop).
	Next(context.Context) (roachpb.RangeDescriptor, error)

	// Close destroys the iterator, releasing resources. It does not need to be
	// called after Next() indicates exhaustion by returning an empty descriptor,
	// or after Next() returns an error.
	Close(context.Context)
}

// meta2RangeIter is an implementation of RangeIterator that scans meta2 in a
// paginated way.
type meta2RangeIter struct {
	db *kv.DB
	// The size of the batches that descriptors will be read in. 0 for no limit.
	batchSize int

	txn *kv.Txn
	// buffer contains descriptors read in the first batch, but not yet returned
	// to the client.
	buffer []kv.KeyValue
	// resumeSpan maintains the point where the meta2 scan stopped.
	resumeSpan *roachpb.Span
	// readingDone is set once we've scanned all of meta2. buffer may still
	// contain descriptors.
	readingDone bool
}

func makeMeta2RangeIter(db *kv.DB, batchSize int) meta2RangeIter {
	return meta2RangeIter{db: db, batchSize: batchSize}
}

var _ RangeIterator = &meta2RangeIter{}

// Next is part of the rangeIterator interface.
func (r *meta2RangeIter) Next(ctx context.Context) (_ roachpb.RangeDescriptor, retErr error) {
	defer func() { r.handleErr(ctx, retErr) }()

	rd, err := r.consumerBuffer()
	if err != nil || rd.RangeID != 0 {
		return rd, err
	}

	if r.readingDone {
		// No more batches to read.
		return roachpb.RangeDescriptor{}, nil
	}

	// Read a batch and consume the first row (if any).
	if err := r.readBatch(ctx); err != nil {
		return roachpb.RangeDescriptor{}, err
	}
	return r.consumerBuffer()
}

func (r *meta2RangeIter) consumerBuffer() (roachpb.RangeDescriptor, error) {
	if len(r.buffer) == 0 {
		return roachpb.RangeDescriptor{}, nil
	}
	first := r.buffer[0]
	var desc roachpb.RangeDescriptor
	if err := first.ValueProto(&desc); err != nil {
		return roachpb.RangeDescriptor{}, errors.NewAssertionErrorWithWrappedErrf(err,
			"%s: unable to unmarshal range descriptor", first.Key)
	}
	r.buffer = r.buffer[1:]
	return desc, nil
}

// Close is part of the RangeIterator interface.
func (r *meta2RangeIter) Close(ctx context.Context) {
	if r.readingDone {
		return
	}
	_ = r.txn.Rollback(ctx)
	r.txn = nil
	r.readingDone = true
}

func (r *meta2RangeIter) readBatch(ctx context.Context) (retErr error) {
	defer func() { r.handleErr(ctx, retErr) }()

	if len(r.buffer) > 0 {
		log.Fatalf(ctx, "buffer not exhausted: %d keys remaining", len(r.buffer))
	}
	if r.txn == nil {
		r.txn = r.db.NewTxn(ctx, "rangeStoreImpl")
		// Set a fixed timestamp to disable uncertainty intervals. This forgoes
		// linearizability (which isn't at all important for this use case) for the
		// guarantee that no retryable errors will be returned, so we don't need to
		// worry about handling them in order to maintain a consistent view across
		// batches. Uncertainty errors are the only form of retryable error that can
		// be returned for read-only transactions.
		if err := r.txn.SetFixedTimestamp(ctx, r.txn.ReadTimestamp()); err != nil {
			return err
		}
	}

	b := r.txn.NewBatch()
	start := keys.Meta2Prefix
	if r.resumeSpan != nil {
		start = r.resumeSpan.Key
	}
	b.Scan(start, keys.MetaMax)
	b.Header.MaxSpanRequestKeys = int64(r.batchSize)
	err := r.txn.Run(ctx, b)
	if err != nil {
		return err
	}
	r.buffer = b.Results[0].Rows
	r.resumeSpan = b.Results[0].ResumeSpan
	if r.resumeSpan == nil {
		if err := r.txn.Commit(ctx); err != nil {
			return err
		}
		r.txn = nil
		r.readingDone = true
	}
	return nil
}

// handleErr manipulates the iterator's state in response to an error.
// Resources are released and the iterator shouldn't be used any more.
// A nil error may be passed, in which case handleErr is a no-op.
//
// handleErr is idempotent.
func (r *meta2RangeIter) handleErr(ctx context.Context, err error) {
	if err == nil {
		return
	}
	if errors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
		log.Warningf(ctx, "unexpected retryable error from "+
			"read-only transaction with fixed read timestamp: %s", err)
	}
	if r.txn != nil {
		if rollbackErr := r.txn.Rollback(ctx); rollbackErr != nil {
			log.Eventf(ctx, "rollback failed: %s", rollbackErr)
		}
		r.txn = nil
	}
	r.reset()
	r.readingDone = true
}

// reset the iterator. The next Next() call will return the first range.
func (r *meta2RangeIter) reset() {
	r.buffer = nil
	r.resumeSpan = nil
	r.readingDone = false
}

type reportID int

// getReportGenerationTime returns the time at a particular report was last
// generated. Returns time.Time{} if the report is not found.
func getReportGenerationTime(
	ctx context.Context, rid reportID, ex isql.Executor, txn *kv.Txn,
) (time.Time, error) {
	row, err := ex.QueryRowEx(
		ctx,
		"get-previous-timestamp",
		txn,
		sessiondata.NodeUserSessionDataOverride,
		"select generated from system.reports_meta where id = $1",
		rid,
	)
	if err != nil {
		return time.Time{}, err
	}

	if row == nil {
		return time.Time{}, nil
	}

	if len(row) != 1 {
		return time.Time{}, errors.AssertionFailedf(
			"expected 1 column from intenal query, got: %d", len(row))
	}
	generated, ok := row[0].(*tree.DTimestampTZ)
	if !ok {
		return time.Time{}, errors.AssertionFailedf("expected to get timestamptz from "+
			"system.reports_meta got %+v (%T)", row[0], row[0])
	}
	return generated.Time, nil
}
