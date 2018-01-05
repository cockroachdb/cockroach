// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acceptanceccl

// The benchmarks in this file are remote tests that use Terrafarm to manage
// and run tests against dedicated test clusters. See allocator_test.go for
// instructions on how to set this up to run locally. Also note that you likely
// want to increase `-benchtime` to something more like 5m (the default is 1s).

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance"
	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	longWaitTime            = 2 * time.Minute
	bulkArchiveStoreFixture = "store-dumps/10nodes-2t-50000ranges"
)

type benchmarkTest struct {
	b testing.TB
	// nodes is the number of nodes this cluster will have.
	nodes int
	// prefix is the prefix that will be prepended to all resources created by
	// Terraform.
	prefix string
	// storeFixture is the name of the Azure Storage fixture to download and use
	// as the store. Nothing is downloaded if storeFixture is empty.
	storeFixture    string
	skipClusterInit bool

	f *terrafarm.Farmer
}

func (bt *benchmarkTest) Start(ctx context.Context) {
	licenseKey, ok := envutil.EnvString("COCKROACH_DEV_LICENSE", 0)
	if !ok {
		bt.b.Fatal("testing enterprise features requires setting COCKROACH_DEV_LICENSE")
	}
	bt.f = acceptance.MakeFarmer(bt.b, bt.prefix, acceptance.GetStopper())
	bt.f.SkipClusterInit = bt.skipClusterInit
	bt.f.TerraformArgs = []string{
		// Ls-series VMs are "storage optimized," which means they have large local
		// SSDs (678GB on the L4s) and no disk throttling beyond the physical limit
		// of the disk. All other VM series on Azure are subject to VM-level I/O
		// throttling, where sync latencies jump to upwards of a 1s when the rate
		// limit is exceeded. Since we expect syncs to take a few milliseconds at
		// most, this causes cascading liveness failures which tank a restore.
		//
		// To determine the temporary SSD I/O rate limit for a particular VM size,
		// look for the "max cached [and temp storage] throughput" column on the
		// Azure VM size chart [0].
		//
		// TODO(benesch): build a rate-limiter that rate limits all write traffic
		// (not just bulk I/O write traffic like kv.bulk_io_write.max_rate), and
		// additionally run these tests on D-series VMs with a suitable rate limit.
		//
		// [0]: https://docs.microsoft.com/en-us/azure/virtual-machines/linux/sizes
		"-var", "azure_vm_size=Standard_L4s",
		"-var", "azure_location=westus",
		// Keep this in sync with build/teamcity-reset-nightlies.sh.
		"-var", "azure_vhd_storage_account=cockroachnightlywestvhd",
	}

	log.Infof(ctx, "creating cluster with %d node(s)", bt.nodes)
	if err := bt.f.Resize(bt.nodes); err != nil {
		bt.b.Fatal(err)
	}

	// TODO(benesch): avoid duplicating all this logic with allocator_test.
	if bt.storeFixture != "" {
		// We must stop the cluster because we're about to overwrite the data
		// directory.
		log.Info(ctx, "stopping cluster")
		for i := 0; i < bt.f.NumNodes(); i++ {
			if err := bt.f.Kill(ctx, i); err != nil {
				bt.b.Fatalf("error stopping node %d: %s", i, err)
			}
		}

		log.Infof(ctx, "downloading archived stores %s in parallel", bt.storeFixture)
		errors := make(chan error, bt.f.NumNodes())
		for i := 0; i < bt.f.NumNodes(); i++ {
			go func(nodeNum int) {
				errors <- bt.f.Exec(nodeNum,
					fmt.Sprintf("find %[1]s -type f -delete && curl -sfSL %s/store%d.tgz | tar -C %[1]s -zx",
						"/mnt/data0", acceptance.FixtureURL(bt.storeFixture), nodeNum+1,
					),
				)
			}(i)
		}
		for i := 0; i < bt.f.NumNodes(); i++ {
			if err := <-errors; err != nil {
				bt.b.Fatalf("error downloading store %d: %s", i, err)
			}
		}

		log.Info(ctx, "restarting cluster with archived store(s)")
		for i := 0; i < bt.f.NumNodes(); i++ {
			if err := bt.f.Restart(ctx, i); err != nil {
				bt.b.Fatalf("error restarting node %d: %s", i, err)
			}
		}
	}
	if err := acceptance.CheckGossip(ctx, bt.f, longWaitTime, acceptance.HasPeers(bt.nodes)); err != nil {
		bt.b.Fatal(err)
	}
	bt.f.Assert(ctx, bt.b)

	sqlDBRaw, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		bt.b.Fatal(err)
	}
	defer sqlDBRaw.Close()
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(bt.b, `SET CLUSTER SETTING cluster.organization = "Cockroach Labs - Production Testing"`)
	sqlDB.Exec(bt.b, fmt.Sprintf(`SET CLUSTER SETTING enterprise.license = "%s"`, licenseKey))
	sqlDB.Exec(bt.b, `SET CLUSTER SETTING trace.debug.enable = 'true'`)
	sqlDB.Exec(bt.b, `SET CLUSTER SETTING server.remote_debugging.mode = 'any'`)
	// On Azure, if we don't limit our disk throughput, we'll quickly cause node
	// liveness failures. This limit was determined experimentally on
	// Standard_D3_v2 instances.
	sqlDB.Exec(bt.b, `SET CLUSTER SETTING kv.bulk_io_write.max_rate = '30MB'`)
	// Stats-based replica and lease rebalancing interacts badly with restore's
	// splits and scatters.
	//
	// TODO(benesch): Remove these settings when #17671 is fixed, or document the
	// necessity of these settings as 1.1 known limitation.
	sqlDB.Exec(bt.b, `SET CLUSTER SETTING kv.allocator.stat_based_rebalancing.enabled = false`)
	sqlDB.Exec(bt.b, `SET CLUSTER SETTING kv.allocator.load_based_lease_rebalancing.enabled = false`)

	log.Info(ctx, "initial cluster is up")
}

func (bt *benchmarkTest) Close(ctx context.Context) {
	if r := recover(); r != nil {
		bt.b.Errorf("recovered from panic to destroy cluster: %v", r)
	}
	if bt.f != nil {
		log.Infof(ctx, "shutting down cluster")
		bt.f.MustDestroy(bt.b)
	}
}

const (
	backupRestoreRowPayloadSize = 100

	// TODO(mjibson): attempt to unify these with the identical ones in sqlccl.
	bankCreateDatabase = `CREATE DATABASE data`
	bankCreateTable    = `CREATE TABLE data.bank (
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`
	bankInsert = `INSERT INTO data.bank VALUES (%d, %d, '%s')`
)

func getAzureURI(t testing.TB, accountName, accountKeyVar, container, object string) string {
	accountKey := os.Getenv(accountKeyVar)
	if accountKey == "" {
		t.Fatalf("env var %s must be set", accountKeyVar)
	}
	return (&url.URL{
		Scheme: "azure",
		Host:   container,
		Path:   object,
		RawQuery: url.Values{
			storageccl.AzureAccountNameParam: []string{accountName},
			storageccl.AzureAccountKeyParam:  []string{accountKey},
		}.Encode(),
	}).String()
}

func getAzureBackupFixtureURI(t testing.TB, name string) string {
	return getAzureURI(
		t, acceptance.FixtureStorageAccount(), "AZURE_FIXTURE_ACCOUNT_KEY", "backups", name,
	)
}

func getAzureEphemeralURI(b *testing.B) string {
	name := fmt.Sprintf("%s/%s-%d", b.Name(), timeutil.Now().Format(time.RFC3339Nano), b.N)
	return getAzureURI(
		b, acceptance.EphemeralStorageAccount(), "AZURE_EPHEMERAL_ACCOUNT_KEY", "backups", name,
	)
}

// BenchmarkRestoreBig creates a backup via Load with b.N rows then benchmarks
// the time to restore it.
func BenchmarkRestoreBig(b *testing.B) {
	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	bt := benchmarkTest{
		b:      b,
		nodes:  3,
		prefix: "restore",
	}

	defer bt.Close(ctx)
	bt.Start(ctx)

	sqlDB, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatal(err)
	}
	defer sqlDB.Close()

	r := sqlutils.MakeSQLRunner(sqlDB)

	r.Exec(b, bankCreateDatabase)

	// (mis-)Use a sub benchmark to avoid running the setup code more than once.
	b.Run("", func(b *testing.B) {
		var buf bytes.Buffer
		buf.WriteString(bankCreateTable)
		buf.WriteString(";\n")
		for i := 0; i < b.N; i++ {
			payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
			fmt.Fprintf(&buf, bankInsert, i, 0, payload)
			buf.WriteString(";\n")
		}

		ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
		restoreURI := getAzureEphemeralURI(b)
		desc, err := sqlccl.Load(ctx, sqlDB, &buf, "data", restoreURI, ts, 0, os.TempDir())
		if err != nil {
			b.Fatal(err)
		}

		dbName := fmt.Sprintf("bank %d", b.N)
		r.Exec(b, fmt.Sprintf("CREATE DATABASE %s", dbName))

		b.ResetTimer()
		log.Infof(ctx, "starting restore to %s", dbName)
		r.Exec(b, fmt.Sprintf(`RESTORE TABLE data.* FROM $1 WITH OPTIONS ('into_db'='%s')`, dbName), restoreURI)
		b.SetBytes(desc.EntryCounts.DataSize / int64(b.N))
		log.Infof(ctx, "restored %s", humanizeutil.IBytes(desc.EntryCounts.DataSize))
		b.StopTimer()
	})
}

func BenchmarkRestoreTPCH10(b *testing.B) {
	for _, numNodes := range []int{1, 3, 10} {
		b.Run(fmt.Sprintf("numNodes=%d", numNodes), func(b *testing.B) {
			if b.N != 1 {
				b.Fatal("b.N must be 1")
			}

			bt := benchmarkTest{
				b:      b,
				nodes:  numNodes,
				prefix: "restore-tpch10",
			}

			ctx := context.Background()
			bt.Start(ctx)
			defer bt.Close(ctx)

			db, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			if _, err := db.Exec("CREATE DATABASE tpch"); err != nil {
				b.Fatal(err)
			}

			fn := func(ctx context.Context) error {
				_, err := db.Exec(`RESTORE tpch.* FROM $1`, getAzureBackupFixtureURI(b, "tpch10"))
				return err
			}
			b.ResetTimer()
			jobID, status, err := jobs.RunAndWaitForTerminalState(ctx, db, fn)
			b.StopTimer()
			if err != nil {
				b.Fatalf("%+v", err)
			}
			if status != jobs.StatusSucceeded {
				b.Fatalf("job %d: expected %s got %s", jobID, jobs.StatusSucceeded, status)
			}
		})
	}
}

func BenchmarkRestore2TB(b *testing.B) {
	if b.N != 1 {
		b.Fatal("b.N must be 1")
	}

	bt := benchmarkTest{
		b: b,
		// TODO(dan): Switch this back to 10 machines when this test goes back
		// to Azure. Each GCE machine disk is 375GB, so with our current need
		// for 2x the restore size in disk space, 2tb unless we up this a bit.
		nodes:  15,
		prefix: "restore2tb",
	}

	ctx := context.Background()
	bt.Start(ctx)
	defer bt.Close(ctx)

	db, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatalf("%+v", err)
	}
	defer db.Close()

	// We're currently pinning this test to be run on GCE via
	// teamcity-nightly-acceptance.sh. Effectively remove the setting for GCE,
	// which doesn't seem to need it.
	if _, err := db.Exec(`SET CLUSTER SETTING kv.bulk_io_write.max_rate = '1GB'`); err != nil {
		b.Fatalf("%+v", err)
	}

	if _, err := db.Exec("CREATE DATABASE datablocks"); err != nil {
		b.Fatalf("%+v", err)
	}

	fn := func(ctx context.Context) error {
		_, err := db.Exec(`RESTORE datablocks.* FROM $1`, `gs://cockroach-test/2t-backup`)
		return err
	}
	b.ResetTimer()
	jobID, status, err := jobs.RunAndWaitForTerminalState(ctx, db, fn)
	b.StopTimer()
	if err != nil {
		b.Fatalf("%+v", err)
	}
	if status != jobs.StatusSucceeded {
		b.Fatalf("job %d: expected %s got %s", jobID, jobs.StatusSucceeded, status)
	}
}

func BenchmarkBackup2TB(b *testing.B) {
	if b.N != 1 {
		b.Fatal("b.N must be 1")
	}

	bt := benchmarkTest{
		b:               b,
		nodes:           10,
		storeFixture:    acceptance.FixtureURL(bulkArchiveStoreFixture),
		prefix:          "backup2tb",
		skipClusterInit: true,
	}

	ctx := context.Background()
	bt.Start(ctx)
	defer bt.Close(ctx)

	db, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	log.Infof(ctx, "starting backup")
	row := db.QueryRow(`BACKUP DATABASE datablocks TO $1`, getAzureEphemeralURI(b))
	var unused string
	var dataSize int64
	if err := row.Scan(&unused, &unused, &unused, &unused, &unused, &unused, &dataSize); err != nil {
		bt.b.Fatal(err)
	}
	b.SetBytes(dataSize)
	log.Infof(ctx, "backed up %s", humanizeutil.IBytes(dataSize))
}
