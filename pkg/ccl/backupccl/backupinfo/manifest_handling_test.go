// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestManifestHandlingIteratorOperations tests operations for iterators over
// the external SSTs of a backup manifest.
func TestManifestHandlingIteratorOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numFiles = 10
	const numDescriptors = 10
	const changesPerDescriptor = 3

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///foo",
		base.ExternalIODirConfig{},
		tc.Server(0).ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		tc.Server(0).InternalDB().(isql.DB),
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

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	store, err := cloud.ExternalStorageFromURI(ctx, "userfile:///foo",
		base.ExternalIODirConfig{},
		tc.Server(0).ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		username.RootUserName(),
		tc.Server(0).InternalDB().(isql.DB),
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
