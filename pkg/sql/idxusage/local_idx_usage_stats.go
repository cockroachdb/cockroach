// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxusage

import (
	"context"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// usageType is the enum specifying the type of usage of an index.
type usageType int8

const (
	// readOp indicates that a read operation has occurred for an index.
	readOp usageType = iota

	// writeOp indicates that a write operation has occurred for an index.
	writeOp
)

// indexUse is the payload struct that record a single instance of the index
// usage.
type indexUse struct {
	// key is what specify a particular index. It's a tuple of
	// (table_id, index_id).
	key roachpb.IndexUsageKey

	// usageTyp specifies how this index is being used.
	usageTyp usageType
}

// LocalIndexUsageStats is a node-local provider of index usage statistics.
// It implements both the idxusage.Reader and idxusage.Writer interfaces.
//
// NOTE: The index usage statistics is collected asynchronously by running a
// statistics ingestion goroutine in the background. This is to avoid lock
// contention during the critical path of query execution. This struct has the
// same lifetime as the sql.Server and the Start() method should be called as
// soon as possible to start the background ingestion goroutine.
type LocalIndexUsageStats struct {
	// statsChan the channel which all index usage metadata are being passed
	// through.
	statsChan chan indexUse

	st *cluster.Settings

	mu struct {
		syncutil.RWMutex

		// usageStats stores index usage statistics per unique roachpb.TableID.
		usageStats map[roachpb.TableID]*tableIndexStats
	}

	// testingKnobs provide utilities for tests to hook into the internal states
	// of the LocalIndexUsageStats.
	testingKnobs *TestingKnobs
}

// tableIndexStats tracks index usage statistics per table.
type tableIndexStats struct {
	syncutil.RWMutex

	tableID roachpb.TableID

	// stats contains the usage information per unique roachpb.IndexID.
	stats map[roachpb.IndexID]*indexStats
}

// indexStats track index usage statistics per index.
type indexStats struct {
	syncutil.RWMutex
	roachpb.IndexUsageStatistics
}

// Config is the configuration struct used to instantiate the LocalIndexUsageStats.
type Config struct {
	// ChannelSize is the size of buffered channel for the statsChan in
	// LocalIndexUsageStats.
	ChannelSize uint64

	// Setting is used to read cluster settings.
	Setting *cluster.Settings

	// Knobs is the testing knobs used for tests.
	Knobs *TestingKnobs
}

// IteratorOptions provides knobs to change the iterating behavior when
// calling ForEach.
type IteratorOptions struct {
	SortedTableID bool
	SortedIndexID bool
	Max           *uint64
}

// StatsVisitor is the callback invoked when calling ForEach.
type StatsVisitor func(key *roachpb.IndexUsageKey, value *roachpb.IndexUsageStatistics) error

// DefaultChannelSize is the default size of the statsChan.
const DefaultChannelSize = uint64(128)

var emptyIndexUsageStats roachpb.IndexUsageStatistics

// NewLocalIndexUsageStats returns a new instance of LocalIndexUsageStats.
func NewLocalIndexUsageStats(cfg *Config) *LocalIndexUsageStats {
	is := &LocalIndexUsageStats{
		statsChan:    make(chan indexUse, cfg.ChannelSize),
		st:           cfg.Setting,
		testingKnobs: cfg.Knobs,
	}
	is.mu.usageStats = make(map[roachpb.TableID]*tableIndexStats)

	return is
}

// NewLocalIndexUsageStatsFromExistingStats returns a new instance of
// LocalIndexUsageStats that is populated using given
// []roachpb.CollectedIndexUsageStatistics. This constructor can be used to
// quickly aggregate the index usage statistics received from the RPC fanout
// and it is more efficient than the regular insert path because it performs
// insert without taking the RWMutex lock.
func NewLocalIndexUsageStatsFromExistingStats(
	cfg *Config, stats []roachpb.CollectedIndexUsageStatistics,
) *LocalIndexUsageStats {
	s := NewLocalIndexUsageStats(cfg)
	s.batchInsertUnsafe(stats)
	return s
}

// Start starts the background goroutine that is responsible for collecting
// index usage statistics.
func (s *LocalIndexUsageStats) Start(ctx context.Context, stopper *stop.Stopper) {
	s.startStatsIngestionLoop(ctx, stopper)
}

// RecordRead records a read operation on the specified index.
func (s *LocalIndexUsageStats) RecordRead(ctx context.Context, key roachpb.IndexUsageKey) {
	s.record(ctx, indexUse{
		key:      key,
		usageTyp: readOp,
	})
}

func (s *LocalIndexUsageStats) record(ctx context.Context, payload indexUse) {
	// If the index usage stats collection s disabled, we abort.
	if !Enable.Get(&s.st.SV) {
		return
	}
	select {
	case s.statsChan <- payload:
	default:
		if log.V(1 /* level */) {
			log.Infof(ctx, "index usage stats provider channel full, discarding new stats")
		}
	}
}

// Get returns the index usage statistics for a given key.
func (s *LocalIndexUsageStats) Get(
	tableID roachpb.TableID, indexID roachpb.IndexID,
) roachpb.IndexUsageStatistics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	table, ok := s.mu.usageStats[tableID]
	if !ok {
		// We return a copy of the empty stats.
		emptyStats := emptyIndexUsageStats
		return emptyStats
	}

	table.RLock()
	defer table.RUnlock()

	indexStats, ok := table.stats[indexID]
	if !ok {
		emptyStats := emptyIndexUsageStats
		return emptyStats
	}

	// Take the read lock while returning the internal data.
	indexStats.RLock()
	defer indexStats.RUnlock()
	return indexStats.IndexUsageStatistics
}

// ForEach iterates through all stored index usage statistics
// based on the options specified in IteratorOptions. If an error is
// encountered when calling StatsVisitor, the iteration is aborted.
func (s *LocalIndexUsageStats) ForEach(options IteratorOptions, visitor StatsVisitor) error {
	maxIterationLimit := uint64(math.MaxUint64)
	if options.Max != nil {
		maxIterationLimit = *options.Max
	}

	s.mu.RLock()
	var tableIDLists []roachpb.TableID
	for tableID := range s.mu.usageStats {
		tableIDLists = append(tableIDLists, tableID)
	}

	if options.SortedTableID {
		sort.Slice(tableIDLists, func(i, j int) bool {
			return tableIDLists[i] < tableIDLists[j]
		})
	}

	s.mu.RUnlock()

	for _, tableID := range tableIDLists {
		tableIdxStats := s.getStatsForTableID(tableID, false /* createIfNotExists */, false /* unsafe */)

		// This means the data s being cleared before we can fetch it. It's not an
		// error, so we simply just skip over it.
		if tableIdxStats == nil {
			continue
		}

		var err error
		maxIterationLimit, err = tableIdxStats.iterateIndexStats(options.SortedIndexID, maxIterationLimit, visitor)
		if err != nil {
			return errors.Wrap(err, "unexpected error encountered when iterating through index usage stats")
		}
		// If we have already reached iterating limit, we abort iteration.
		if maxIterationLimit == 0 {
			break
		}
	}

	return nil
}

// batchInsertUnsafe inserts otherStats into s without taking on write lock.
// This should only be called during initialization when we can be sure there's
// no other users of s. This avoids the locking overhead when it's not
// necessary.
func (s *LocalIndexUsageStats) batchInsertUnsafe(
	otherStats []roachpb.CollectedIndexUsageStatistics,
) {
	for _, newStats := range otherStats {
		tableIndexStats := s.getStatsForTableID(newStats.Key.TableID, true /* createIfNotExists */, true /* unsafe */)
		stats := tableIndexStats.getStatsForIndexID(newStats.Key.IndexID, true /* createIfNotExists */, true /* unsafe */)
		stats.Add(&newStats.Stats)
	}
}

func (s *LocalIndexUsageStats) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tableStats := range s.mu.usageStats {
		tableStats.clear()
	}
}

func (s *LocalIndexUsageStats) insertIndexUsage(idxUse *indexUse) {
	tableStats := s.getStatsForTableID(idxUse.key.TableID, true /* createIfNotExists */, false /* unsafe */)
	indexStats := tableStats.getStatsForIndexID(idxUse.key.IndexID, true /* createIfNotExists */, false /* unsafe */)
	indexStats.Lock()
	defer indexStats.Unlock()
	switch idxUse.usageTyp {
	// TODO(azhng): include TotalRowsRead/TotalRowsWritten field once it s plumbed
	//  into the SQL engine.
	case readOp:
		indexStats.TotalReadCount++
		indexStats.LastRead = timeutil.Now()
		// TODO(azhng): include TotalRowsRead field once it s plumbed into
		//  the exec engine.
	case writeOp:
		indexStats.TotalWriteCount++
		indexStats.LastWrite = timeutil.Now()
	}
}

// getStatsForTableID returns the tableIndexStats for the given roachpb.TableID.
// If unsafe is set to true, then the lookup is performed without locking to the
// internal RWMutex lock. This can be used when LocalIndexUsageStats is not
// being concurrently accessed.
func (s *LocalIndexUsageStats) getStatsForTableID(
	id roachpb.TableID, createIfNotExists bool, unsafe bool,
) *tableIndexStats {
	if !unsafe {
		if createIfNotExists {
			s.mu.Lock()
			defer s.mu.Unlock()
		} else {
			s.mu.RLock()
			defer s.mu.RUnlock()
		}
	}

	if tableIndexStats, ok := s.mu.usageStats[id]; ok {
		return tableIndexStats
	}

	if createIfNotExists {
		newTableIndexStats := &tableIndexStats{
			tableID: id,
			stats:   make(map[roachpb.IndexID]*indexStats),
		}
		s.mu.usageStats[id] = newTableIndexStats
		return newTableIndexStats
	}

	return nil
}

// getStatsForIndexID returns the indexStats for the given roachpb.IndexID.
// If unsafe is set to true, then the lookup is performed without locking to the
// internal RWMutex lock. This can be used when tableIndexStats is not being
// concurrently accessed.
func (t *tableIndexStats) getStatsForIndexID(
	id roachpb.IndexID, createIfNotExists bool, unsafe bool,
) *indexStats {
	if !unsafe {
		if createIfNotExists {
			t.Lock()
			defer t.Unlock()
		} else {
			t.RLock()
			defer t.RUnlock()
		}

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
	orderedIndexID bool, iterLimit uint64, visitor StatsVisitor,
) (newIterLimit uint64, err error) {
	var indexIDs []roachpb.IndexID
	t.RLock()
	for indexID := range t.stats {
		if iterLimit == 0 {
			break
		}
		indexIDs = append(indexIDs, indexID)
		iterLimit--
	}
	t.RUnlock()

	if orderedIndexID {
		sort.Slice(indexIDs, func(i, j int) bool {
			return indexIDs[i] < indexIDs[j]
		})
	}

	for _, indexID := range indexIDs {
		indexStats := t.getStatsForIndexID(indexID, false /* createIfNotExists */, false /* unsafe */)

		// This means the data is being cleared  before we can fetch it. It's not an
		// error, so we simply just skip over it.
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
			return 0 /* newIterLimit */, err
		}
	}
	return iterLimit, nil
}

func (t *tableIndexStats) clear() {
	t.Lock()
	defer t.Unlock()

	t.stats = make(map[roachpb.IndexID]*indexStats, len(t.stats)/2)
}

func (s *LocalIndexUsageStats) startStatsIngestionLoop(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "index-usage-stats-ingest", func(ctx context.Context) {
		for {
			select {
			case payload := <-s.statsChan:
				s.insertIndexUsage(&payload)
				if s.testingKnobs != nil && s.testingKnobs.OnIndexUsageStatsProcessedCallback != nil {
					s.testingKnobs.OnIndexUsageStatsProcessedCallback(payload.key)
				}
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	})
}
