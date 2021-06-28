// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexstatslocal

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/indexusagestats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// indexUsageStats is a node-local implementation of indexusagestats.Provider.
type indexUsageStats struct {
	// statsChan the channel which all index usage metadata are being passed
	// through.
	statsChan chan indexusagestats.MetaData

	st *cluster.Settings

	mu struct {
		syncutil.RWMutex

		usageStats map[roachpb.TableID]*tableIndexStats

		lastReset time.Time
	}

	// testingKnobs provide utilities for tests to hook into the internal states
	// of the indexUsageStats.
	testingKnobs *TestingKnobs
}

// tableIndexStats tracks index usage stats per table.
type tableIndexStats struct {
	syncutil.RWMutex

	tableID roachpb.TableID
	stats   map[roachpb.IndexID]*indexStats
}

type indexStats struct {
	syncutil.RWMutex
	roachpb.IndexUsageStatistics
}

// Config is the configuration struct used to instantiate the indexUsageStats.
type Config struct {
	// ChannelSize is the size of buffered channel for the statsChan in
	// indexUsageStats.
	ChannelSize uint64

	// Setting is used to read cluster settings.
	Setting *cluster.Settings

	// Knobs is the testing knobs used for tests.
	Knobs *TestingKnobs
}

// DefaultChannelSize is the default size of the statsChan.
const DefaultChannelSize = uint64(128)

var _ indexusagestats.Provider = &indexUsageStats{}

var emptyIndexUsageStats roachpb.IndexUsageStatistics

// New returns a new instance of indexUsageStats.
func New(cfg *Config) indexusagestats.Provider {
	is := &indexUsageStats{
		statsChan:    make(chan indexusagestats.MetaData, cfg.ChannelSize),
		st:           cfg.Setting,
		testingKnobs: cfg.Knobs,
	}
	is.mu.usageStats = make(map[roachpb.TableID]*tableIndexStats)

	return is
}

// Start implements the indexusagestats.Provider interface.
func (is *indexUsageStats) Start(ctx context.Context, stopper *stop.Stopper) {
	is.startStatsIngestionLoop(ctx, stopper)
}

// Record implements the indexusagestats.Provider interface.
func (is *indexUsageStats) Record(ctx context.Context, payload indexusagestats.MetaData) {
	select {
	case is.statsChan <- payload:
	default:
		if log.V(2 /* level */) {
			log.Warningf(ctx, "index usage stats provider channel full, discarding new stats")
		}
	}
}

// GetIndexUsageStats implements the indexusagestats.Provider interface.
func (is *indexUsageStats) GetIndexUsageStats(
	key roachpb.IndexUsageKey,
) roachpb.IndexUsageStatistics {
	is.mu.RLock()
	defer is.mu.RUnlock()

	table, ok := is.mu.usageStats[key.TableID]
	if !ok {
		// We return a copy of the empty stats.
		emptyStats := emptyIndexUsageStats
		return emptyStats
	}

	table.RLock()
	defer table.RUnlock()

	indexStats, ok := table.stats[key.IndexID]
	if !ok {
		emptyStats := emptyIndexUsageStats
		return emptyStats
	}

	// Take the read lock while returning the internal data.
	indexStats.RLock()
	defer indexStats.RUnlock()
	return indexStats.IndexUsageStatistics
}

// IterateIndexUsageStats the indexusagestats.Provider interface.
func (is *indexUsageStats) IterateIndexUsageStats(
	options indexusagestats.IteratorOptions, visitor indexusagestats.StatsVisitor,
) error {
	is.mu.RLock()

	var tableIDLists []roachpb.TableID
	for tableID := range is.mu.usageStats {
		tableIDLists = append(tableIDLists, tableID)
	}

	if options.SortedTableID {
		sort.Slice(tableIDLists, func(i, j int) bool {
			return tableIDLists[i] < tableIDLists[j]
		})
	}

	is.mu.RUnlock()

	curIter := uint64(0)
	for _, tableID := range tableIDLists {
		tableIdxStats := is.getStatsForTableID(tableID, false /* createIfNotExists */)

		// This means the data is being cleared by the reset loop before we can
		// fetch it. It's not an error, so we simply just skip over it.
		if tableIdxStats == nil {
			continue
		}

		earlyStopped, err := tableIdxStats.iterateIndexStats(options.SortedIndexID, &curIter, options.Max, visitor)
		if err != nil {
			return fmt.Errorf("unexpected error encountered when iterating through index usage stats: %s", err)
		}
		// If we have already reached iterating limit, we abort iteration.
		if earlyStopped {
			break
		}
	}

	return nil
}

// GetLastReset implements the indexusagestats.Provider interface;
func (is *indexUsageStats) GetLastReset() time.Time {
	is.mu.RLock()
	defer is.mu.RUnlock()
	return is.mu.lastReset
}

// IngestStats implements the indexusagestats.Provider interface.
func (is *indexUsageStats) IngestStats(otherStats []roachpb.CollectedIndexUsageStatistics) {
	for _, newStats := range otherStats {
		tableIndexStats := is.getStatsForTableID(newStats.Key.TableID, true /* createIfNotExists */)
		stats := tableIndexStats.getStatsForIndexID(newStats.Key.IndexID, true /* createIfNotExists */)
		stats.Lock()
		stats.Add(&newStats.Stats)
		stats.Unlock()
	}
}

// Clear implements the indexusagestats.Provider interface.
func (is *indexUsageStats) Clear() {
	is.mu.Lock()
	defer is.mu.Unlock()

	for _, tableStats := range is.mu.usageStats {
		tableStats.clear()
	}

	is.mu.lastReset = timeutil.Now()
}

func (is *indexUsageStats) insertIndexUsage(payload *indexusagestats.MetaData) {
	tableStats := is.getStatsForTableID(payload.Key.TableID, true /* createIfNotExists */)
	indexStats := tableStats.getStatsForIndexID(payload.Key.IndexID, true /* createIfNotExists */)
	indexStats.Lock()
	defer indexStats.Unlock()
	switch payload.UsageType {
	// TODO(azhng): include TotalRowsRead/TotalRowsWrite field once it is plumbed
	//  into the SQL engine.
	case indexusagestats.ReadOp:
		indexStats.TotalReadCount++
		indexStats.LastRead = timeutil.Now()
		// TODO(azhng): include TotalRowsRead field once it is plumbed into
		//  the exec engine.
	case indexusagestats.WriteOp:
		indexStats.TotalWriteCount++
		indexStats.LastWrite = timeutil.Now()
	}
}

func (is *indexUsageStats) getStatsForTableID(
	id roachpb.TableID, createIfNotExists bool,
) *tableIndexStats {
	if createIfNotExists {
		is.mu.Lock()
		defer is.mu.Unlock()
	} else {
		is.mu.RLock()
		defer is.mu.RUnlock()
	}

	if tableIndexStats, ok := is.mu.usageStats[id]; ok {
		return tableIndexStats
	}

	if createIfNotExists {
		newTableIndexStats := &tableIndexStats{
			tableID: id,
			stats:   make(map[roachpb.IndexID]*indexStats),
		}
		is.mu.usageStats[id] = newTableIndexStats
		return newTableIndexStats
	}

	return nil
}

func (t *tableIndexStats) getStatsForIndexID(
	id roachpb.IndexID, createIfNotExists bool,
) *indexStats {
	if createIfNotExists {
		t.Lock()
		defer t.Unlock()
	} else {
		t.RLock()
		defer t.RUnlock()
	}

	if stats, ok := t.stats[id]; ok {
		return stats
	}
	if createIfNotExists {
		newUsageEntry := &indexStats{}
		t.stats[id] = newUsageEntry
		return newUsageEntry
	}
	return nil
}

func (t *tableIndexStats) iterateIndexStats(
	orderedIndexID bool, curIterVal, max *uint64, visitor indexusagestats.StatsVisitor,
) (earlyStopped bool, err error) {
	var indexIDs []roachpb.IndexID
	t.RLock()
	for indexID := range t.stats {
		if max != nil && *curIterVal >= *max {
			earlyStopped = true
			break
		}
		indexIDs = append(indexIDs, indexID)
		*curIterVal++
	}
	t.RUnlock()

	if orderedIndexID {
		sort.Slice(indexIDs, func(i, j int) bool {
			return indexIDs[i] < indexIDs[j]
		})
	}

	for _, indexID := range indexIDs {
		indexStats := t.getStatsForIndexID(indexID, false /* createIfNotExists */)

		// This means the data is being cleared by the reset loop before we can
		// fetch it. It's not an error, so we simply just skip over it.
		if indexStats == nil {
			continue
		}

		indexStats.RLock()
		// Copy out the stats while holding read lock.
		statsCopy := indexStats.IndexUsageStatistics
		indexStats.RUnlock()

		if err := visitor(&roachpb.IndexUsageKey{
			TableID: t.tableID,
			IndexID: indexID,
		}, &statsCopy); err != nil {
			return earlyStopped, err
		}
	}
	return earlyStopped, nil
}

func (t *tableIndexStats) clear() {
	t.Lock()
	defer t.Unlock()

	t.stats = make(map[roachpb.IndexID]*indexStats, len(t.stats)/2)
}

func (is *indexUsageStats) startStatsIngestionLoop(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "index-usage-stats-ingest", func(ctx context.Context) {
		for {
			select {
			case payload := <-is.statsChan:
				// If the index usage stats collection is disabled, we abort.
				if !indexusagestats.Enable.Get(&is.st.SV) {
					continue
				}
				is.insertIndexUsage(&payload)
				if is.testingKnobs != nil && is.testingKnobs.OnIndexUsageStatsProcessedCallback != nil {
					is.testingKnobs.OnIndexUsageStatsProcessedCallback(payload)
				}
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	})
}
