// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo_test

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestManifestHandlingIteratorOperations tests operations for iterators over
// the external SSTs of a backup manifest.
func TestManifestHandlingIteratorOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numFiles = 10
	const numDescriptors = 10
	const changesPerDescriptor = 3

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0).ApplicationLayer()
	store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///foo",
		base.ExternalIODirConfig{},
		s.ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		s.InternalDB().(isql.DB),
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)

	m := makeMockManifest(numFiles, numDescriptors, changesPerDescriptor)
	require.NoError(t, backupinfo.WriteMetadataWithExternalSSTs(ctx, store, nil, nil, &m))

	iterFactory := backupinfo.NewIterFactory(&m, store, nil, nil)

	fileLess := func(left backuppb.BackupManifest_File, right backuppb.BackupManifest_File) bool {
		return backupinfo.FileCmp(left, right) < 0
	}
	var sortedFiles []backuppb.BackupManifest_File
	sortedFiles = append(sortedFiles, m.Files...)
	sort.Slice(sortedFiles, func(i, j int) bool {
		return fileLess(m.Files[i], m.Files[j])
	})

	descLess := func(left descpb.Descriptor, right descpb.Descriptor) bool {
		tLeft, _, _, _, _ := descpb.GetDescriptors(&left)
		tRight, _, _, _, _ := descpb.GetDescriptors(&right)
		return tLeft.ID < tRight.ID
	}
	var sortedDescs []descpb.Descriptor
	sortedDescs = append(sortedDescs, m.Descriptors...)
	sort.Slice(sortedDescs, func(i, j int) bool {
		return descLess(sortedDescs[i], sortedDescs[j])
	})

	descRevsLess := func(
		left backuppb.BackupManifest_DescriptorRevision,
		right backuppb.BackupManifest_DescriptorRevision,
	) bool {
		return backupinfo.DescChangesLess(&left, &right)
	}
	var sortedDescRevs []backuppb.BackupManifest_DescriptorRevision
	sortedDescRevs = append(sortedDescRevs, m.DescriptorChanges...)
	sort.Slice(sortedDescRevs, func(i, j int) bool {
		return descRevsLess(sortedDescRevs[i], sortedDescRevs[j])
	})

	t.Run("files", func(t *testing.T) {
		checkIteratorOperations(t, mustCreateFileIterFactory(t, iterFactory), sortedFiles, fileLess)
	})
	t.Run("descriptors", func(t *testing.T) {
		checkIteratorOperations(t, iterFactory.NewDescIter, sortedDescs, descLess)
	})
	t.Run("descriptor-changes", func(t *testing.T) {
		checkIteratorOperations(t, iterFactory.NewDescriptorChangesIter, sortedDescRevs, descRevsLess)
	})
}

// TestManifestHandlingIteratorOperations tests operations for an empty external
// manifest SST iterator.
func TestManifestHandlingEmptyIterators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0).ApplicationLayer()
	store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///foo",
		base.ExternalIODirConfig{},
		s.ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		s.InternalDB().(isql.DB),
		nil, /* limiters */
		cloud.NilMetrics,
	)
	require.NoError(t, err)

	m := makeMockManifest(0, 0, 0)
	require.NoError(t, backupinfo.WriteMetadataWithExternalSSTs(ctx, store, nil, nil, &m))

	iterFactory := backupinfo.NewIterFactory(&m, store, nil, nil)
	t.Run("files", func(t *testing.T) {
		checkEmptyIteratorOperations(t, mustCreateFileIterFactory(t, iterFactory))
	})
	t.Run("descriptors", func(t *testing.T) {
		checkEmptyIteratorOperations(t, iterFactory.NewDescIter)
	})
	t.Run("descriptor-changes", func(t *testing.T) {
		checkEmptyIteratorOperations(t, iterFactory.NewDescriptorChangesIter)
	})
}

func makeMockManifest(
	numFiles int, numDescriptors int, changesPerDescriptor int,
) backuppb.BackupManifest {
	m := backuppb.BackupManifest{}
	m.HasExternalManifestSSTs = true
	m.MVCCFilter = backuppb.MVCCFilter_All
	for i := 0; i < numFiles; i++ {
		spKey := fmt.Sprintf("/Table/%04d", i)
		spEndKey := fmt.Sprintf("/Table/%04d", i+1)
		f := backuppb.BackupManifest_File{
			Span: roachpb.Span{
				Key:    []byte(spKey),
				EndKey: []byte(spEndKey),
			},
			Path: fmt.Sprintf("file%04d.sst", i),
		}
		m.Files = append(m.Files, f)
	}

	for i := 1; i <= numDescriptors; i++ {
		// Have some deleted descriptors as well.
		isDeleted := i%5 == 4

		tbl := descpb.TableDescriptor{ID: descpb.ID(i),
			Name:    fmt.Sprintf("table%d", i),
			Version: descpb.DescriptorVersion(changesPerDescriptor),
		}
		desc := descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &tbl}}
		if !isDeleted {
			m.Descriptors = append(m.Descriptors, desc)
		}

		for j := 1; j <= changesPerDescriptor; j++ {
			tbl.Version = descpb.DescriptorVersion(j)
			rev := backuppb.BackupManifest_DescriptorRevision{
				Time: hlc.Timestamp{WallTime: int64(j)},
				ID:   tbl.ID,
				Desc: &desc,
			}

			if isDeleted && j == changesPerDescriptor {
				rev.Desc = nil
			}
			m.DescriptorChanges = append(m.DescriptorChanges, rev)
		}
	}

	return m
}

func checkIteratorOperations[T any](
	t *testing.T,
	mkIter func(context.Context) bulk.Iterator[*T],
	expected []T,
	less func(left T, right T) bool,
) {
	ctx := context.Background()

	// 1. Check if the iterator returns the expected contents, regardless of how
	// many times value is called between calls to Next().
	for numValueCalls := 1; numValueCalls <= 5; numValueCalls++ {
		var actual []T
		it := mkIter(ctx)
		defer it.Close()
		for ; ; it.Next() {
			if ok, err := it.Valid(); err != nil {
				t.Fatal(err)
			} else if !ok {
				break
			}

			var value T
			for i := 0; i < numValueCalls; i++ {
				value = *it.Value()
			}

			actual = append(actual, value)
		}

		sort.Slice(actual, func(i, j int) bool {
			return less(actual[i], actual[j])
		})

		require.Equal(t, expected, actual, fmt.Sprintf("contents not equal if there are %d calls to Value()", numValueCalls))
	}

	// 2. Check that we can repeatedly call Next() and Value() after the iterator
	// is done.
	it := mkIter(ctx)
	defer it.Close()
	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
	}

	for i := 0; i < 10; i++ {
		it.Next()
		ok, err := it.Valid()
		require.False(t, ok)
		require.NoError(t, err)

		it.Value() // Should not error or panic.
	}

	// 3. Check that we can get the value without calling Valid().
	itNoCheck := mkIter(ctx)
	defer itNoCheck.Close()
	require.Greater(t, len(expected), 0)
	value := itNoCheck.Value()
	require.Contains(t, expected, *value)

	ok, err := itNoCheck.Valid()
	require.True(t, ok)
	require.NoError(t, err)
}

func checkEmptyIteratorOperations[T any](
	t *testing.T, mkIter func(context.Context) bulk.Iterator[*T],
) {
	ctx := context.Background()

	// Check that regardless of how many calls to Next() the iterator will not be
	// valid.
	for numNextCalls := 0; numNextCalls < 5; numNextCalls++ {
		it := mkIter(ctx)
		defer it.Close()
		for i := 0; i < numNextCalls; i++ {
			it.Next()
		}

		ok, err := it.Valid()
		require.NoError(t, err)
		require.False(t, ok)

		it.Value() // Should not error or panic.
	}
}

func mustCreateFileIterFactory(
	t *testing.T, iterFactory *backupinfo.IterFactory,
) func(ctx context.Context) bulk.Iterator[*backuppb.BackupManifest_File] {
	return func(ctx context.Context) bulk.Iterator[*backuppb.BackupManifest_File] {
		it, err := iterFactory.NewFileIter(ctx)
		require.NoError(t, err)
		return it
	}
}

func TestMakeBackupCodec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tenID, err := roachpb.MakeTenantID(10)
	require.NoError(t, err)
	tenSpan := keys.MakeTenantSpan(tenID)
	for _, tc := range []struct {
		name          string
		manifests     []backuppb.BackupManifest
		expectedCodec keys.SQLCodec
	}{
		{
			name: "full",
			manifests: []backuppb.BackupManifest{
				{Spans: []roachpb.Span{{Key: roachpb.Key("/Table/123")}}},
			},
			expectedCodec: keys.SystemSQLCodec,
		},
		{
			name: "full-backup-tenant",
			manifests: []backuppb.BackupManifest{
				{Spans: []roachpb.Span{tenSpan}},
			},
			expectedCodec: keys.MakeSQLCodec(tenID),
		},
		{
			name: "full-backup-of-tenant",
			manifests: []backuppb.BackupManifest{
				{
					Spans:   []roachpb.Span{tenSpan},
					Tenants: []mtinfopb.TenantInfoWithUsage{{SQLInfo: mtinfopb.SQLInfo{ID: 10}}},
				},
			},
			expectedCodec: keys.SystemSQLCodec,
		},
		{
			name: "empty-full-backup",
			manifests: []backuppb.BackupManifest{
				{Spans: []roachpb.Span{}},
				{Spans: []roachpb.Span{{Key: roachpb.Key("/Table/123")}}},
			},
			expectedCodec: keys.SystemSQLCodec,
		},
		{
			name: "empty-full-backup-tenant",
			manifests: []backuppb.BackupManifest{
				{Spans: []roachpb.Span{}},
				{Spans: []roachpb.Span{tenSpan}},
			},
			expectedCodec: keys.MakeSQLCodec(tenID),
		},
		{
			name: "empty-full-backup-of-tenant",
			manifests: []backuppb.BackupManifest{
				{
					Spans:   []roachpb.Span{},
					Tenants: []mtinfopb.TenantInfoWithUsage{{SQLInfo: mtinfopb.SQLInfo{ID: 10}}},
				},
				{
					Spans:   []roachpb.Span{tenSpan},
					Tenants: []mtinfopb.TenantInfoWithUsage{{SQLInfo: mtinfopb.SQLInfo{ID: 10}}},
				},
			},
			expectedCodec: keys.SystemSQLCodec,
		},
		{
			name: "all-empty",
			manifests: []backuppb.BackupManifest{
				{Spans: []roachpb.Span{{}}},
				{Spans: []roachpb.Span{{}}},
				{Spans: []roachpb.Span{{}}},
			},
			expectedCodec: keys.SystemSQLCodec,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := backupinfo.MakeBackupCodec(tc.manifests)
			require.NoError(t, err)
			require.Equal(t, tc.expectedCodec, c)
		})
	}
}

func TestValidateEndTimeAndTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	m := func(start, end int, compacted bool, revision bool) backuppb.BackupManifest {
		b := backuppb.BackupManifest{
			StartTime:   hlc.Timestamp{WallTime: int64(start)},
			EndTime:     hlc.Timestamp{WallTime: int64(end)},
			IsCompacted: compacted,
		}
		if revision {
			b.MVCCFilter = backuppb.MVCCFilter_All
			b.RevisionStartTime = hlc.Timestamp{WallTime: int64(start)}
		}
		return b
	}
	mNorm := func(start, end int) backuppb.BackupManifest {
		return m(start, end, false /* compacted */, false /* revision */)
	}
	mComp := func(start, end int) backuppb.BackupManifest {
		return m(start, end, true /* compacted */, false /* revision */)
	}
	mRev := func(start, end int) backuppb.BackupManifest {
		return m(start, end, false /* compacted */, true /* revision */)
	}

	// Note: The tests here work under the assumption that the input manifests are
	// always sorted in ascending order by end time, and then sorted in ascending
	// order by start time.
	for _, tc := range []struct {
		name             string
		manifests        []backuppb.BackupManifest
		endTime          int
		includeCompacted bool
		err              string
		expected         [][]int // expected timestamps of returned backups
	}{
		{
			name: "single backup",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1),
			},
			endTime:  1,
			expected: [][]int{{0, 1}},
		},
		{
			name: "double backup",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2),
			},
			endTime:  2,
			expected: [][]int{{0, 1}, {1, 2}},
		},
		{
			name: "out of bounds end time",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2),
			},
			endTime: 3,
			err:     "supplied backups do not cover requested time",
		},
		{
			name: "revision history restore should fail on non-revision history backups",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 2), mNorm(2, 4),
			},
			endTime: 3,
			err:     "restoring to arbitrary time",
		},
		{
			name: "revision history restore should succeed on revision history backups",
			manifests: []backuppb.BackupManifest{
				mRev(0, 2), mRev(2, 4),
			},
			endTime:  3,
			expected: [][]int{{0, 2}, {2, 4}},
		},
		{
			name: "end time in middle of chain should truncate",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mNorm(2, 3),
				mNorm(3, 5), mNorm(5, 8),
			},
			endTime:  3,
			expected: [][]int{{0, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "non-continuous backup chain should fail",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(2, 3),
			},
			endTime: 3,
			err:     "backups are not continuous",
		},
		{
			name: "ignore compacted backups if includeCompacted is false",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mComp(1, 3), mNorm(2, 3),
			},
			endTime:  3,
			expected: [][]int{{0, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "compaction of two backups",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mComp(1, 3), mNorm(2, 3),
				mNorm(3, 5), mNorm(5, 8),
			},
			endTime:          8,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 3}, {3, 5}, {5, 8}},
		},
		{
			name: "compaction of entire incremental chain",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mNorm(2, 3), mNorm(3, 5),
				mComp(1, 8), mNorm(5, 8),
			},
			endTime:          8,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 8}},
		},
		{
			name: "two separate compactions of two backups",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mComp(1, 3), mNorm(2, 3),
				mNorm(3, 5), mComp(3, 8), mNorm(5, 8),
			},
			endTime:          8,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 3}, {3, 8}},
		},
		{
			name: "compaction includes a compacted backup in the middle",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mComp(1, 3), mNorm(2, 3),
				mNorm(3, 5), mComp(1, 8), mNorm(5, 8),
			},
			endTime:          8,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 8}},
		},
		{
			name: "two compactions with the same end time",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mNorm(2, 3), mNorm(3, 5),
				mComp(1, 8), mComp(3, 8), mNorm(5, 8),
			},
			endTime:          8,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 8}},
		},
		{
			name: "end time in middle of compacted chain should pick base incremental",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mNorm(2, 3),
				mComp(1, 5), mNorm(3, 5),
			},
			endTime:          3,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "overlapping compacted backups",
			manifests: []backuppb.BackupManifest{
				mNorm(0, 1), mNorm(1, 2), mComp(1, 3), mNorm(2, 3), mComp(2, 4), mNorm(3, 4),
			},
			endTime:          4,
			includeCompacted: true,
			expected:         [][]int{{0, 1}, {1, 2}, {2, 4}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Using the expected start and end times, create a slice containing the
			// expected expectedOrder of the manifests based on their original indexes. For
			// example, [2, 0, 1] means that the expected output should should have
			// the third input manifest first, the first input manifest second, and the
			// second input manifest last.
			expectedOrder := util.Map(tc.expected, func(ts []int) string {
				return strconv.Itoa(slices.IndexFunc(tc.manifests, func(m backuppb.BackupManifest) bool {
					return m.StartTime.WallTime == int64(ts[0]) && m.EndTime.WallTime == int64(ts[1])
				}))
			})
			// Create URIs and locality info in a way that we can track their
			// resulting order after being truncated. We set input URIs to their
			// stringified index, and locality info maps to a map containing just
			// their index. We can later use the expectedOrder to check that the
			// output maps to the expected indexes.
			inputURIs := make([]string, len(tc.manifests))
			inputLocs := make([]jobspb.RestoreDetails_BackupLocalityInfo, len(tc.manifests))
			for i := range tc.manifests {
				index := strconv.Itoa(i)
				inputURIs[i] = index
				inputLocs[i].URIsByOriginalLocalityKV = make(map[string]string)
				inputLocs[i].URIsByOriginalLocalityKV[index] = index
			}

			uris, res, locs, err := backupinfo.ValidateEndTimeAndTruncate(
				inputURIs, tc.manifests, inputLocs,
				hlc.Timestamp{WallTime: int64(tc.endTime)},
				false, /* includeSkipped */
				tc.includeCompacted,
			)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.Equal(t, len(tc.expected), len(res))
			require.Equal(t, expectedOrder, uris)
			require.Len(t, locs, len(tc.expected))
			for idx, rank := range expectedOrder {
				_, ok := locs[idx].URIsByOriginalLocalityKV[rank]
				require.True(t, ok)
			}

			for i := range tc.expected {
				actual := []int{int(res[i].StartTime.WallTime), int(res[i].EndTime.WallTime)}
				require.Equal(t, tc.expected[i], actual)
			}
		})
	}
}
