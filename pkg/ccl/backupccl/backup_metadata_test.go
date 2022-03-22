// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestMetadataSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const numAccounts = 1
	userfile := "userfile:///0"
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// Check that backup metadata is correct on full cluster backup.
	sqlDB.Exec(t, `BACKUP TO $1`, userfile)
	checkMetadata(ctx, t, tc, userfile)

	// Check for correct backup metadata on incremental backup with revision
	// history.
	sqlDB.Exec(t, `CREATE TABLE data.foo(k INT, v INT)`)
	sqlDB.Exec(t, `CREATE INDEX idx ON data.bank (balance)`)

	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, userfile)
	checkMetadata(ctx, t, tc, userfile)

	//  Check for correct backup metadata on single table backups.
	userfile1 := "userfile:///1"
	sqlDB.Exec(t, `BACKUP TABLE data.bank TO $1 WITH revision_history`, userfile1)
	checkMetadata(ctx, t, tc, userfile1)

	// Check for correct backup metadata on tenant backups.
	userfile2 := "userfile:///2"
	_, err := tc.Servers[0].StartTenant(ctx, base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	require.NoError(t, err)
	sqlDB.Exec(t, `BACKUP TENANT 10 TO $1`, userfile2)
	checkMetadata(ctx, t, tc, userfile2)
}

func checkMetadata(
	ctx context.Context, t *testing.T, tc *testcluster.TestCluster, backupLoc string,
) {
	store, err := cloud.ExternalStorageFromURI(
		ctx,
		backupLoc,
		base.ExternalIODirConfig{},
		tc.Servers[0].ClusterSettings(),
		blobs.TestEmptyBlobClientFactory,
		security.RootUserName(),
		tc.Servers[0].InternalExecutor().(*sql.InternalExecutor), tc.Servers[0].DB())
	if err != nil {
		t.Fatal(err)
	}
	m, err := testingReadBackupManifest(ctx, store, backupManifestName)
	if err != nil {
		t.Fatal(err)
	}

	bm, err := newBackupMetadata(ctx, store, metadataSSTName, nil)
	if err != nil {
		t.Fatal(err)
	}

	checkManifest(t, m, bm)
	// If there are descriptor changes, we only check those as they should have
	// all changes as well as existing descriptors
	if len(m.DescriptorChanges) > 0 {
		checkDescriptorChanges(ctx, t, m, bm)
	} else {
		checkDescriptors(ctx, t, m, bm)
	}

	checkSpans(ctx, t, m, bm)
	// Don't check introduced spans on the first backup.
	if m.StartTime != (hlc.Timestamp{}) {
		checkIntroducedSpans(ctx, t, m, bm)
	}
	checkFiles(ctx, t, m, bm)
	checkTenants(ctx, t, m, bm)
	checkStats(ctx, t, store, m, bm)
}

func checkManifest(t *testing.T, m *BackupManifest, bm *BackupMetadata) {
	expectedManifest := *m
	expectedManifest.Descriptors = nil
	expectedManifest.DescriptorChanges = nil
	expectedManifest.Files = nil
	expectedManifest.Spans = nil
	expectedManifest.IntroducedSpans = nil
	expectedManifest.StatisticsFilenames = nil
	expectedManifest.Tenants = nil

	require.Equal(t, expectedManifest, bm.BackupManifest)
}

func checkDescriptors(ctx context.Context, t *testing.T, m *BackupManifest, bm *BackupMetadata) {
	var metaDescs []descpb.Descriptor
	var desc descpb.Descriptor

	it := bm.DescIter(ctx)
	defer it.Close()
	for it.Next(&desc) {
		metaDescs = append(metaDescs, desc)
	}

	if it.Err() != nil {
		t.Fatal(it.Err())
	}

	require.Equal(t, m.Descriptors, metaDescs)
}

func checkDescriptorChanges(
	ctx context.Context, t *testing.T, m *BackupManifest, bm *BackupMetadata,
) {
	var metaRevs []BackupManifest_DescriptorRevision
	var rev BackupManifest_DescriptorRevision
	it := bm.DescriptorChangesIter(ctx)
	defer it.Close()

	for it.Next(&rev) {
		metaRevs = append(metaRevs, rev)
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}

	// Descriptor Changes are sorted by time in the manifest.
	sort.Slice(metaRevs, func(i, j int) bool {
		return metaRevs[i].Time.Less(metaRevs[j].Time)
	})

	require.Equal(t, m.DescriptorChanges, metaRevs)
}

func checkFiles(ctx context.Context, t *testing.T, m *BackupManifest, bm *BackupMetadata) {
	var metaFiles []BackupManifest_File
	var file BackupManifest_File
	it := bm.FileIter(ctx)
	defer it.Close()

	for it.Next(&file) {
		metaFiles = append(metaFiles, file)
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}

	require.Equal(t, m.Files, metaFiles)
}

func checkSpans(ctx context.Context, t *testing.T, m *BackupManifest, bm *BackupMetadata) {
	var metaSpans []roachpb.Span
	var span roachpb.Span
	it := bm.SpanIter(ctx)
	defer it.Close()

	for it.Next(&span) {
		metaSpans = append(metaSpans, span)
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}

	require.Equal(t, m.Spans, metaSpans)
}

func checkIntroducedSpans(
	ctx context.Context, t *testing.T, m *BackupManifest, bm *BackupMetadata,
) {
	var metaSpans []roachpb.Span
	var span roachpb.Span
	it := bm.IntroducedSpanIter(ctx)
	defer it.Close()
	for it.Next(&span) {
		metaSpans = append(metaSpans, span)
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}

	require.Equal(t, m.IntroducedSpans, metaSpans)
}

func checkTenants(ctx context.Context, t *testing.T, m *BackupManifest, bm *BackupMetadata) {
	var metaTenants []descpb.TenantInfoWithUsage
	var tenant descpb.TenantInfoWithUsage
	it := bm.TenantIter(ctx)
	defer it.Close()

	for it.Next(&tenant) {
		metaTenants = append(metaTenants, tenant)
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}

	require.Equal(t, m.Tenants, metaTenants)
}

func checkStats(
	ctx context.Context,
	t *testing.T,
	store cloud.ExternalStorage,
	m *BackupManifest,
	bm *BackupMetadata,
) {
	expectedStats, err := getStatisticsFromBackup(ctx, store, nil, *m)
	if err != nil {
		t.Fatal(err)
	}

	var metaStats = make([]*stats.TableStatisticProto, 0)
	var s *stats.TableStatisticProto
	it := bm.StatsIter(ctx)
	defer it.Close()

	for it.Next(&s) {
		metaStats = append(metaStats, s)
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	require.Equal(t, expectedStats, metaStats)
}

func testingReadBackupManifest(
	ctx context.Context, store cloud.ExternalStorage, file string,
) (*BackupManifest, error) {
	r, err := store.ReadFile(ctx, file)
	if err != nil {
		return nil, err
	}
	defer r.Close(ctx)

	bytes, err := ioctx.ReadAll(ctx, r)
	if err != nil {
		return nil, err
	}
	if isGZipped(bytes) {
		descBytes, err := decompressData(ctx, nil, bytes)
		if err != nil {
			return nil, err
		}
		bytes = descBytes
	}

	var m BackupManifest
	if err := protoutil.Unmarshal(bytes, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
