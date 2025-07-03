// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
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

func TestBackupCompactionHeuristic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testcases := []struct {
		name       string
		sizes      []int64
		windowSize int
		expected   [2]int
	}{
		{
			name:       "optimal at the beginning",
			sizes:      []int64{1, 2, 2, 5, 1, 2},
			windowSize: 3,
			expected:   [2]int{0, 3},
		},
		{
			name:       "optimal in the middle",
			sizes:      []int64{1, 3, 4, 5, 5, 2},
			windowSize: 3,
			expected:   [2]int{2, 5},
		},
		{
			name:       "optimal at the end",
			sizes:      []int64{1, 3, 4, 3, 2, 2},
			windowSize: 3,
			expected:   [2]int{3, 6},
		},
		{
			name:       "tied heuristic",
			sizes:      []int64{2, 3, 4, 1, 2, 3},
			windowSize: 3,
			expected:   [2]int{0, 3},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := minDeltaWindow(tc.sizes, tc.windowSize)
			require.Equal(t, tc.expected[0], start)
			require.Equal(t, tc.expected[1], end)
		})
	}

	st := cluster.MakeTestingClusterSettings()
	t.Run("too large window", func(t *testing.T) {
		var windowSize int64 = 5
		chain := make([]backuppb.BackupManifest, 5)
		backupCompactionWindow.Override(ctx, &st.SV, windowSize)
		execCfg := &sql.ExecutorConfig{Settings: st}
		_, _, err := minSizeDeltaHeuristic(ctx, execCfg, chain)
		require.Error(t, err)
	})
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
				compacted, chain, err := chain.Compact(t, ctx, execCfg, policy)
				require.NoError(t, err)

				t.Logf(
					"%s:\n\tchain size: %d\n\tlast backup size: %d\n\tcompacted size: %d",
					tc.name, chain.Size(), chain[len(chain)-1].Size(), compacted.Size(),
				)
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
	updateableKeys := allKeys[:]

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
// a compacted backup and the backups that were compacted.
func (c fakeBackupChain) Compact(
	t *testing.T, ctx context.Context, execCfg *sql.ExecutorConfig, policy compactionPolicy,
) (fakeBackup, []fakeBackup, error) {
	manifests := c.toBackupManifests()
	start, end, err := policy(ctx, execCfg, manifests)
	if err != nil {
		return fakeBackup{}, nil, err
	}
	t.Logf("Compacting backups from index %d to %d", start, end)
	compacted, err := c.compactWindow(start, end)
	if err != nil {
		return fakeBackup{}, nil, err
	}
	return compacted, c[start:end], nil
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
