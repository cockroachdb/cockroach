// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMinSumWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := []struct {
		name       string
		nums       []int64
		windowSize int
		expected   [2]int
		notFound   bool
	}{
		{
			name:       "no invalids",
			nums:       []int64{4, 2, 7, 3, 6},
			windowSize: 1,
			expected:   [2]int{1, 2},
		},
		{
			name:       "no invalids/optimal at beginning",
			nums:       []int64{1, 2, 7, 3, 6, 8, 2, 4},
			windowSize: 3,
			expected:   [2]int{0, 3},
		},
		{
			name:       "no invalids/optimal in middle",
			nums:       []int64{4, 5, 1, 2, 3, 6, 7},
			windowSize: 3,
			expected:   [2]int{2, 5},
		},
		{
			name:       "no invalids/optimal at end",
			nums:       []int64{5, 6, 7, 2, 1, 3},
			windowSize: 3,
			expected:   [2]int{3, 6},
		},
		{
			name:       "with invalids/single invalid at beginning",
			nums:       []int64{math.MaxInt64, 2, 3, 1, 4},
			windowSize: 3,
			expected:   [2]int{1, 4},
		},
		{
			name:       "with invalids/double invalid to skip",
			nums:       []int64{math.MaxInt64, math.MaxInt64, 5, 1, 3, 4},
			windowSize: 3,
			expected:   [2]int{3, 6},
		},
		{
			name:       "with invalids/double invalid with gap",
			nums:       []int64{1, math.MaxInt64, 2, math.MaxInt64, 3, 4, 5, 1, 6, 7},
			windowSize: 3,
			expected:   [2]int{5, 8},
		},
		{
			name:       "no valid window/all invalids",
			nums:       []int64{math.MaxInt64, math.MaxInt64, math.MaxInt64},
			windowSize: 2,
			notFound:   true,
		},
		{
			name:       "no valid window/interspersed invalids",
			nums:       []int64{1, math.MaxInt64, 2, math.MaxInt64, 3},
			windowSize: 2,
			notFound:   true,
		},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%s/window=%d", tc.name, tc.windowSize), func(t *testing.T) {
			start, end, ok := minSumWindow(tc.nums, tc.windowSize)
			if tc.notFound {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				require.Equal(t, tc.expected, [2]int{start, end}, "start/end did not match")
			}
		})
	}
}

func TestMinSizeDeltaHeuristic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	testcases := []struct {
		name string
		// backupSizes represents the size of the incremental backups in the chain.
		backupSizes []int
		windowSize  int
		concurrency int
		// Expected indices of the selected windows of the backups specified in
		// backupSizes.
		expected [][2]int
		errMsg   string
	}{
		{
			name:        "simple case",
			backupSizes: []int{1, 2, 3, 4, 5, 6, 7, 8},
			windowSize:  3,
			concurrency: 1,
			expected:    [][2]int{{0, 3}},
		},
		{
			name:        "non-overlapping min windows",
			backupSizes: []int{1, 5, 3, 30, 40, 50, 3, 2, 1, 50},
			windowSize:  3,
			concurrency: 2,
			expected:    [][2]int{{6, 9}, {0, 3}},
		},
		{
			// Chain where two minimum windows overlap, so only one can be chosen.
			name:        "two overlapping min windows",
			backupSizes: []int{40, 2, 3, 1, 4, 5, 8, 50, 60, 70},
			windowSize:  3,
			concurrency: 2,
			expected:    [][2]int{{1, 4}, {4, 7}},
		},
		{
			name:        "several overlapping min windows",
			backupSizes: []int{10, 2, 1, 4, 1, 1, 3, 1, 2, 3, 10},
			windowSize:  3,
			concurrency: 3,
			expected:    [][2]int{{4, 7}, {7, 10}, {1, 4}},
		},
		{
			name:        "multiple windows and all backups",
			backupSizes: []int{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5},
			windowSize:  3,
			concurrency: 5,
			expected:    [][2]int{{0, 3}, {3, 6}, {6, 9}, {9, 12}, {12, 15}},
		},
		{
			// Due to the greedy nature of the heuristic, it may select an initial
			// window that prevents other windows from being selected, resulting in
			// less than the max concurrency.
			name:        "first window blocks further concurrency",
			backupSizes: []int{5, 1, 1, 1, 1, 5},
			windowSize:  3,
			concurrency: 2,
			expected:    [][2]int{{1, 4}},
		},
		{
			name:        "chain length blocks further concurrency",
			backupSizes: []int{1, 2, 3, 4, 5, 6},
			windowSize:  3,
			concurrency: 3,
			expected:    [][2]int{{0, 3}, {3, 6}},
		},
		{
			name:        "invalid window size",
			backupSizes: []int{1, 2, 3, 4, 5},
			windowSize:  6,
			concurrency: 1,
			errMsg:      "compaction window size must be less than backup chain length",
		},
	}

	sizesToWorkload := func(backupSizes ...int) []*fakeWorkloadCfg {
		workloads := make([]*fakeWorkloadCfg, len(backupSizes))
		for i, size := range backupSizes {
			// The type of workload doesn't matter for this test, only the size of the
			// backup.
			workloads[i] = newWorkloadCfg(randomWorkload{updateProbability: 0.5}).Backups(1).Keys(size)
		}
		return workloads
	}

	for _, tc := range testcases {
		t.Run(
			fmt.Sprintf("%s/window=%d/concurrency=%d", tc.name, tc.windowSize, tc.concurrency),
			func(t *testing.T) {
				backupChain := newFakeBackupChainFactory(t, nil, 100).AddWorkload(
					sizesToWorkload(tc.backupSizes...)...,
				).CreateBackupChain()
				backupCompactionWindow.Override(ctx, &st.SV, int64(tc.windowSize))
				backupCompactionConcurrency.Override(ctx, &st.SV, int64(tc.concurrency))
				execCfg := &sql.ExecutorConfig{Settings: st}
				startEndPairs, err := minSizeDeltaHeuristic(
					ctx, execCfg, backupChain.toBackupManifests(),
				)
				if tc.errMsg != "" {
					require.ErrorContains(t, err, tc.errMsg)
					return
				}
				defer func() {
					if t.Failed() {
						backupSizes := make([]int, len(backupChain))
						for i, backup := range backupChain {
							backupSizes[i] = backup.Size()
						}
						t.Logf("backup sizes: %v", backupSizes)
					}
				}()
				require.NoError(t, err)
				require.Len(t, startEndPairs, len(tc.expected), "number of windows did not match")
				// Check that each result pair is expected.
				for _, pair := range startEndPairs {
					// Because the indices from the pairs include the full backup, we
					// offset by one to match the expected indices.
					offsetPair := [2]int{pair[0] - 1, pair[1] - 1}
					require.Contains(t, tc.expected, offsetPair, "unexpected window indices")
				}
			},
		)
	}
}

func TestSimulateCompactionPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %d", seed)
	policies := map[string]compactionPolicy{
		"min size delta": minSizeDeltaHeuristic,
	}

	appendOnly := randomWorkload{updateProbability: 0.0}
	updateOnly := randomWorkload{updateProbability: 1.0}
	evenWorkload := randomWorkload{updateProbability: 0.5}

	testcases := []struct {
		name       string
		factory    *fakeBackupChainFactory
		windowSize int64
	}{
		{
			name: "append-only workload",
			factory: newFakeBackupChainFactory(t, rng, 100).
				AddWorkload(newWorkloadCfg(appendOnly).Backups(10).Keys(20)),
		},
		{
			name: "update-only workload",
			factory: newFakeBackupChainFactory(t, rng, 100).
				AddWorkload(newWorkloadCfg(updateOnly).Backups(10).Keys(20)),
		},
		{
			name: "mixed workload",
			factory: newFakeBackupChainFactory(t, rng, 100).
				AddWorkload(newWorkloadCfg(evenWorkload).Backups(10).Keys(20)),
		},
		{
			name: "update same small keyspace workload",
			factory: newFakeBackupChainFactory(t, rng, 100).AddWorkload(
				newWorkloadCfg(appendOnly).Backups(1).Keys(20),
				newWorkloadCfg(updateSameKeysWorkload{}).Backups(10).Keys(15),
			),
		},
		{
			name: "append-only workload with varying size update-only workload over small keyspace",
			factory: newFakeBackupChainFactory(t, rng, 100).AddWorkload(
				newWorkloadCfg(appendOnly).Backups(3).Keys(20),
				newWorkloadCfg(updateOnly).Backups(1).Keys(10),
				newWorkloadCfg(updateOnly).Backups(1).Keys(15),
				newWorkloadCfg(updateOnly).Backups(1).Keys(20),
			),
		},
	}
	for policyName, policy := range policies {
		for _, tc := range testcases {
			t.Run(tc.name+" with policy "+policyName, func(t *testing.T) {
				if tc.windowSize <= 0 {
					tc.windowSize = backupCompactionWindow.Default()
				}
				st := cluster.MakeTestingClusterSettings()
				backupCompactionWindow.Override(ctx, &st.SV, tc.windowSize)
				execCfg := &sql.ExecutorConfig{Settings: st}

				chain := tc.factory.CreateBackupChain()
				compacted, indices, err := chain.Compact(t, ctx, execCfg, policy)
				require.NoError(t, err)

				results := "Compaction Results:\n"
				for i, compactedBackup := range compacted {
					startEnd := indices[i]
					results += fmt.Sprintf(
						" \tCompacted Backup %d:\n", i,
					)
					results += fmt.Sprintf(
						"\t\tCompacted Indexes: [%d, %d)\n",
						startEnd[0], startEnd[1],
					)
					results += fmt.Sprintf(
						"\t\tSize of last backup in compaction: %d\n",
						chain[startEnd[1]-1].Size(),
					)
					results += fmt.Sprintf(
						"\t\tCompacted size: %d\n",
						compactedBackup.Size(),
					)
				}
				t.Logf(results)
			})
		}
	}
}

// We choose an integer as a fake key type since it is quickest to generate new
// unique keys for testing purposes.
type fakeKey int64

// fakeBackupChainFactory is a factory that iterates over a set of workloads to
// create a backup chain.
type fakeBackupChainFactory struct {
	t               *testing.T
	rng             *rand.Rand
	initialKeySpace int                // The number of keys in the initial full backup
	workloads       []*fakeWorkloadCfg // Maps a workload to the number of backups it has
}

func newFakeBackupChainFactory(
	t *testing.T, rng *rand.Rand, initialKeySpace int,
) *fakeBackupChainFactory {
	if initialKeySpace <= 0 {
		t.Fatalf("initial key space must be greater than zero, got: %d", initialKeySpace)
	}
	return &fakeBackupChainFactory{
		t:               t,
		rng:             rng,
		initialKeySpace: initialKeySpace,
	}
}

// AddWorkload adds a workload configuration to the factory.
func (f *fakeBackupChainFactory) AddWorkload(
	workloadCfg ...*fakeWorkloadCfg,
) *fakeBackupChainFactory {
	f.workloads = append(f.workloads, workloadCfg...)
	return f
}

// CreateBackupChain generates a fake backup chain based on the configured
// workloads.
func (f *fakeBackupChainFactory) CreateBackupChain() fakeBackupChain {
	if f.rng == nil {
		var seed int64
		f.rng, seed = randutil.NewPseudoRand()
		f.t.Logf("no rng specified, using random seed: %d", seed)
	}
	var totalBackups int
	for _, workload := range f.workloads {
		totalBackups += workload.numBackups
	}

	chain := make(fakeBackupChain, 0, totalBackups+1)
	chain = append(chain, f.initializeFullBackup())

	for _, workload := range f.workloads {
		for range workload.numBackups {
			chain = append(
				chain,
				workload.workload.CreateBackup(f.rng, chain, workload.numKeys),
			)
		}
	}
	return chain
}

// initializeFullBackup creates a full backup with the initial key space defined
// in the factory.
func (f *fakeBackupChainFactory) initializeFullBackup() fakeBackup {
	backup := newFakeBackup()
	key := fakeKey(0)
	for range f.initialKeySpace {
		backup.AddKey(key)
		key++
	}
	return backup
}

// fakeWorkloadCfg specifies the number of backups and keys per backup for a
// specific workload to create.
type fakeWorkloadCfg struct {
	workload   fakeWorkload // The workload to be added
	numBackups int          // The number of backups to create for this workload
	numKeys    int          // The number of keys to write in each backup
}

func newWorkloadCfg(workload fakeWorkload) *fakeWorkloadCfg {
	return &fakeWorkloadCfg{
		workload: workload,
	}
}

// Backups sets the number of backups to create for the workload.
func (c *fakeWorkloadCfg) Backups(count int) *fakeWorkloadCfg {
	c.numBackups = count
	return c
}

// Keys sets the number of keys to write in each backup created by the workload.
func (c *fakeWorkloadCfg) Keys(count int) *fakeWorkloadCfg {
	c.numKeys = count
	return c
}

// fakeWorkload is an interface that defines a method to create a backup based
// on a given workload. It generates a backup based on the provided backup
// chain and the number of keys to write.
type fakeWorkload interface {
	CreateBackup(*rand.Rand, fakeBackupChain, int) fakeBackup
}

// A workload that will randomly append or update keys in a backup.
type randomWorkload struct {
	updateProbability float64 // 0 for append-only, 1 for update-only
}

func (w randomWorkload) CreateBackup(
	rng *rand.Rand, chain fakeBackupChain, numKeys int,
) fakeBackup {
	backup := newFakeBackup()
	allKeys := chain.AllKeys()
	if len(allKeys) == 0 {
		return backup
	}

	// Store keys that can be updated to avoid duplicate updates.
	updateableKeys := slices.Clone(allKeys)

	for range numKeys {
		if rng.Float64() < w.updateProbability && len(updateableKeys) > 0 {
			randIdx := rng.Intn(len(updateableKeys))
			randomKey := updateableKeys[randIdx]
			updateableKeys = slices.Delete(updateableKeys, randIdx, randIdx+1)
			backup.AddKey(randomKey)
		} else {
			newKey := allKeys[len(allKeys)-1] + 1
			backup.AddKey(newKey)
			allKeys = append(allKeys, newKey)
		}
	}
	return backup
}

// A workload that only updates keys that were written in the previous backup.
type updateSameKeysWorkload struct{}

func (w updateSameKeysWorkload) CreateBackup(
	rng *rand.Rand, chain fakeBackupChain, numKeys int,
) fakeBackup {
	backup := newFakeBackup()
	lastBackup := chain[len(chain)-1]
	keys := lastBackup.Keys()

	for range numKeys {
		if len(keys) == 0 {
			break
		}
		randIdx := rng.Intn(len(keys))
		randomKey := keys[randIdx]
		keys = slices.Delete(keys, randIdx, randIdx+1)
		backup.AddKey(randomKey)
	}

	return backup
}

type fakeBackupChain []fakeBackup

// AllKeys returns a sorted list of all unique keys across all backups in the
// chain.
func (c fakeBackupChain) AllKeys() []fakeKey {
	keys := make(map[fakeKey]struct{})
	for _, backup := range c {
		for key := range backup.keys {
			keys[key] = struct{}{}
		}
	}
	allKeys := make([]fakeKey, 0, len(keys))
	for key := range keys {
		allKeys = append(allKeys, key)
	}
	slices.Sort(allKeys)
	return allKeys
}

// Size returns the total number of keys across all backups in the chain,
// counting duplicates.
func (c fakeBackupChain) Size() int {
	totalSize := 0
	for _, backup := range c {
		totalSize += backup.Size()
	}
	return totalSize
}

// Compact applies the provided compaction policy to the backup chain, returning
// the compacted backups and the [start, end) indices of their composite backups.
func (c fakeBackupChain) Compact(
	t *testing.T, ctx context.Context, execCfg *sql.ExecutorConfig, policy compactionPolicy,
) ([]fakeBackup, [][2]int, error) {
	manifests := c.toBackupManifests()
	windows, err := policy(ctx, execCfg, manifests)
	if err != nil {
		return nil, nil, err
	}
	compactedBackups := make([]fakeBackup, 0, len(windows))
	indices := make([][2]int, 0, len(windows))
	for _, window := range windows {
		start, end := window[0], window[1]
		t.Logf("Compacting backups from index %d to %d", start, end)
		compacted, err := c.compactWindow(start, end)
		if err != nil {
			return nil, nil, err
		}
		compactedBackups = append(compactedBackups, compacted)
		indices = append(indices, [2]int{start, end})
	}
	return compactedBackups, indices, nil
}

// compactWindow compacts the backups in the chain from the specified start to
// end indices, returning a new fakeBackup that contains all unique keys from
// the specified range.
func (c fakeBackupChain) compactWindow(start, end int) (fakeBackup, error) {
	if start < 1 || end > len(c) || start >= end {
		return fakeBackup{}, errors.New("invalid window indices")
	}
	backup := newFakeBackup()
	for i := start; i < end; i++ {
		for key := range c[i].keys {
			backup.AddKey(key)
		}
	}
	return backup, nil
}

// toBackupManifests converts the fakeBackupChain into a slice of backup
// manifests to be used in the compaction policy.
func (c fakeBackupChain) toBackupManifests() []backuppb.BackupManifest {
	return util.Map(c, func(backup fakeBackup) backuppb.BackupManifest {
		return backup.toBackupManifest()
	})
}

// fakeBackup represents a backup that contains some set of keys.
// Note: As we write more heuristics, it may be necessary to increase the
// complexity of this struct to include more metadata about the backup that can
// then be translated into backup manifests for the policy to use.
type fakeBackup struct {
	keys map[fakeKey]struct{}
}

func newFakeBackup() fakeBackup {
	return fakeBackup{
		keys: make(map[fakeKey]struct{}),
	}
}

// Size returns the number of unique keys in the backup.
func (m *fakeBackup) Size() int {
	return len(m.keys)
}

// Keys returns a sorted slice of all unique keys in the backup.
func (m *fakeBackup) Keys() []fakeKey {
	keys := make([]fakeKey, 0, len(m.keys))
	for key := range m.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// AddKey adds a key to the backup. If the key already exists, it will not be
// added again, ensuring uniqueness.
func (m *fakeBackup) AddKey(key fakeKey) *fakeBackup {
	m.keys[key] = struct{}{}
	return m
}

// toBackupManifest converts the fakeBackup into a backup manifest.
func (m *fakeBackup) toBackupManifest() backuppb.BackupManifest {
	manifest := backuppb.BackupManifest{
		EntryCounts: roachpb.RowCount{
			DataSize: int64(len(m.keys)),
		},
	}
	return manifest
}
