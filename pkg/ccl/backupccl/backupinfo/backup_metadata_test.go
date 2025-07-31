// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo_test

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestMetadataSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	userfile := "userfile:///0"
	tc, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t,
		backuptestutils.SingleNode,
		backuptestutils.WithBank(1),
		backuptestutils.WithParams(base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			},
		}))
	defer cleanupFn()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulkio.write_metadata_sst.enabled = true`)

	// Check that backup metadata is correct on full cluster backup.
	sqlDB.Exec(t, `BACKUP TO $1`, userfile)
	checkMetadata(ctx, t, tc, userfile)

	// Check for correct backup metadata on incremental backup with revision
	// history.
	sqlDB.Exec(t, `CREATE TABLE data.foo(k INT, v INT)`)
	sqlDB.Exec(t, `CREATE INDEX idx ON data.bank (balance)`)
	sqlDB.Exec(t, `CREATE DATABASE emptydb`)
	sqlDB.Exec(t, `CREATE TABLE emptydb.bar(k INT, v INT)`)
	sqlDB.Exec(t, `DROP DATABASE emptydb`)

	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, userfile)
	checkMetadata(ctx, t, tc, userfile)

	//  Check for correct backup metadata on single table backups.
	userfile1 := "userfile:///1"
	sqlDB.Exec(t, `BACKUP TABLE data.bank TO $1 WITH revision_history`, userfile1)
	checkMetadata(ctx, t, tc, userfile1)

	// Check for correct backup metadata on tenant backups.
	userfile2 := "userfile:///2"
	_, err := tc.Servers[0].TenantController().StartTenant(ctx, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
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
		username.RootUserName(),
		tc.Servers[0].InternalDB().(isql.DB),
		nil, /* limiters */
		cloud.NilMetrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	m, err := testingReadBackupManifest(ctx, store, backupbase.BackupManifestName)
	if err != nil {
		t.Fatal(err)
	}

	srv := tc.Servers[0]
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	kmsEnv := backupencryption.MakeBackupKMSEnv(srv.ClusterSettings(), &base.ExternalIODirConfig{},
		execCfg.InternalDB, username.RootUserName())
	bm, err := backupinfo.NewBackupMetadata(ctx, store, backupinfo.MetadataSSTName,
		nil /* encryption */, &kmsEnv)
	if err != nil {
		t.Fatal(err)
	}

	checkManifest(t, m, bm)
	checkDescriptorChanges(ctx, t, m, bm)
	checkDescriptors(ctx, t, m, bm)
	checkSpans(ctx, t, m, bm)
	// Don't check introduced spans on the first backup.
	if m.StartTime != (hlc.Timestamp{}) {
		checkIntroducedSpans(ctx, t, m, bm)
	}
	checkFiles(ctx, t, m, bm)
	checkTenants(ctx, t, m, bm)
	checkStats(ctx, t, store, m, bm, &kmsEnv)
}

func checkManifest(t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata) {
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

func checkDescriptors(
	ctx context.Context, t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata,
) {
	var metaDescs []descpb.Descriptor

	it := bm.NewDescIter(ctx)
	defer it.Close()
	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}

		metaDescs = append(metaDescs, *it.Value())
	}

	require.Equal(t, m.Descriptors, metaDescs)
}

func checkDescriptorChanges(
	ctx context.Context, t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata,
) {
	var metaRevs []backuppb.BackupManifest_DescriptorRevision
	it := bm.NewDescriptorChangesIter(ctx)
	defer it.Close()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		metaRevs = append(metaRevs, *it.Value())
	}

	// Descriptor Changes are sorted by time in the manifest.
	sort.Slice(metaRevs, func(i, j int) bool {
		return metaRevs[i].Time.Less(metaRevs[j].Time)
	})

	require.Equal(t, m.DescriptorChanges, metaRevs)
}

func checkFiles(
	ctx context.Context, t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata,
) {
	var metaFiles []backuppb.BackupManifest_File
	it, err := bm.NewFileIter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}

		metaFiles = append(metaFiles, *it.Value())
	}

	require.Equal(t, m.Files, metaFiles)
}

func checkSpans(
	ctx context.Context, t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata,
) {
	var metaSpans []roachpb.Span
	it := bm.NewSpanIter(ctx)
	defer it.Close()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}

		metaSpans = append(metaSpans, it.Value())
	}

	require.Equal(t, m.Spans, metaSpans)
}

func checkIntroducedSpans(
	ctx context.Context, t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata,
) {
	var metaSpans []roachpb.Span
	it := bm.NewIntroducedSpanIter(ctx)
	defer it.Close()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		metaSpans = append(metaSpans, it.Value())
	}

	require.Equal(t, m.IntroducedSpans, metaSpans)
}

func checkTenants(
	ctx context.Context, t *testing.T, m *backuppb.BackupManifest, bm *backupinfo.BackupMetadata,
) {
	var metaTenants []mtinfopb.TenantInfoWithUsage
	it := bm.NewTenantIter(ctx)
	defer it.Close()

	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}

		metaTenants = append(metaTenants, it.Value())
	}

	require.Equal(t, m.Tenants, metaTenants)
}

func checkStats(
	ctx context.Context,
	t *testing.T,
	store cloud.ExternalStorage,
	m *backuppb.BackupManifest,
	bm *backupinfo.BackupMetadata,
	kmsEnv cloud.KMSEnv,
) {
	expectedStats, err := backupinfo.GetStatisticsFromBackup(ctx, store, nil, kmsEnv, *m)
	if err != nil {
		t.Fatal(err)
	}
	if len(expectedStats) == 0 {
		expectedStats = nil
	}

	sort.Slice(expectedStats, func(i, j int) bool {
		return expectedStats[i].TableID < expectedStats[j].TableID ||
			(expectedStats[i].TableID == expectedStats[j].TableID && expectedStats[i].StatisticID < expectedStats[j].StatisticID)
	})

	it := bm.NewStatsIter(ctx)
	defer it.Close()
	metaStats, err := bulk.CollectToSlice(it)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, expectedStats, metaStats)
}

func testingReadBackupManifest(
	ctx context.Context, store cloud.ExternalStorage, file string,
) (*backuppb.BackupManifest, error) {
	r, _, err := store.ReadFile(ctx, file, cloud.ReadOptions{NoFileSize: true})
	if err != nil {
		return nil, err
	}
	defer r.Close(ctx)

	bytes, err := ioctx.ReadAll(ctx, r)
	if err != nil {
		return nil, err
	}
	if backupinfo.IsGZipped(bytes) {
		descBytes, err := backupinfo.DecompressData(ctx, mon.NewStandaloneUnlimitedAccount(), bytes)
		if err != nil {
			return nil, err
		}
		bytes = descBytes
	}

	var m backuppb.BackupManifest
	if err := protoutil.Unmarshal(bytes, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
