// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	roachprodErrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type sysbenchWorkload int

const (
	oltpDelete sysbenchWorkload = iota
	oltpInsert
	oltpPointSelect
	oltpUpdateIndex
	oltpUpdateNonIndex
	oltpReadOnly
	oltpReadWrite
	oltpWriteOnly

	numSysbenchWorkloads
)

var sysbenchWorkloadName = map[sysbenchWorkload]string{
	oltpDelete:         "oltp_delete",
	oltpInsert:         "oltp_insert",
	oltpPointSelect:    "oltp_point_select",
	oltpUpdateIndex:    "oltp_update_index",
	oltpUpdateNonIndex: "oltp_update_non_index",
	oltpReadOnly:       "oltp_read_only",
	oltpReadWrite:      "oltp_read_write",
	oltpWriteOnly:      "oltp_write_only",
}

type extraSetup struct {
	nameSuffix string
	stmts      []string
	useDRPC    bool
}

func (w sysbenchWorkload) String() string {
	return sysbenchWorkloadName[w]
}

type sysbenchOptions struct {
	workload     sysbenchWorkload
	distribution string // default `uniform`
	duration     time.Duration
	concurrency  int
	tables       int
	rowsPerTable int
	usePostgres  bool
	extra        extraSetup // invoked before the workload starts
}

func (o *sysbenchOptions) cmd(haproxy bool) string {
	pghost := "{pghost:1}"
	pgport := "{pgport:1}"
	if haproxy {
		pghost = "127.0.0.1"
		pgport = "26257"
	}
	distribution := "uniform"
	if o.distribution != "" {
		distribution = o.distribution
	}
	return fmt.Sprintf(`sysbench \
		--db-driver=pgsql \
		--pgsql-host=%s \
		--pgsql-port=%s \
		--pgsql-user=%s \
		--pgsql-password=%s \
		--pgsql-db=sysbench \
		--report-interval=1 \
		--rand-type=%s \
		--time=%d \
		--threads=%d \
		--tables=%d \
		--table_size=%d \
		--auto_inc=false \
		%s`,
		pghost,
		pgport,
		install.DefaultUser,
		install.DefaultPassword,
		distribution,
		int(o.duration.Seconds()),
		o.concurrency,
		o.tables,
		o.rowsPerTable,
		o.workload,
	)
}

func runSysbench(ctx context.Context, t test.Test, c cluster.Cluster, opts sysbenchOptions) {
	if opts.usePostgres {
		if len(c.CRDBNodes()) != 1 {
			t.Fatal("sysbench with postgres requires exactly one node")
		}
		pgNode := c.CRDBNodes()[:1]

		t.Status("installing postgres")
		if err := c.Install(ctx, t.L(), pgNode, "postgresql"); err != nil {
			t.Fatal(err)
		}
		cmds := []string{
			// Move the data directory to the local SSD.
			`sudo service postgresql stop`,
			`sudo mv /var/lib/postgresql /var/lib/postgresql.bak`,
			`sudo -u postgres mkdir /mnt/data1/postgresql`,
			`sudo ln -s /mnt/data1/postgresql /var/lib/postgresql`,
			`sudo -u postgres cp -R /var/lib/postgresql.bak/* /var/lib/postgresql/`,
			// Allow remote connections.
			`echo "port = 26257"               | sudo tee -a /etc/postgresql/*/main/postgresql.conf`,
			`echo "listen_addresses = '*'"     | sudo tee -a /etc/postgresql/*/main/postgresql.conf`,
			`echo "host all all 0.0.0.0/0 md5" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf`,
			// Start the PG server.
			`sudo service postgresql start`,
			// Create the database and user.
			`sudo -u postgres psql -c "CREATE DATABASE sysbench"`,
			fmt.Sprintf(`sudo -u postgres psql -c "CREATE ROLE %s WITH LOGIN PASSWORD '%s'"`, install.DefaultUser, install.DefaultPassword),
		}
		for _, cmd := range cmds {
			c.Run(ctx, option.WithNodes(pgNode), cmd)
		}
	} else {
		t.Status("installing cockroach")
		settings := install.MakeClusterSettings()
		if opts.extra.useDRPC {
			settings.Env = append(settings.Env, "COCKROACH_EXPERIMENTAL_DRPC_ENABLED=true")
			t.L().Printf("extra setup to use DRPC")
		}
		c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, c.CRDBNodes())
		if len(c.CRDBNodes()) >= 3 {
			err := roachtestutil.WaitFor3XReplication(ctx, t.L(), c.Conn(ctx, t.L(), 1))
			require.NoError(t, err)
		}
		conn := c.Conn(ctx, t.L(), 1)
		runner := sqlutils.MakeSQLRunner(conn)
		runner.Exec(t, `CREATE DATABASE sysbench`)
		for _, stmt := range opts.extra.stmts {
			runner.Exec(t, stmt)
			t.L().Printf(`executed extra setup statement: %s`, stmt)
		}
		_ = conn.Close()
	}

	useHAProxy := len(c.CRDBNodes()) > 1
	if useHAProxy {
		t.Status("installing haproxy")
		if err := c.Install(ctx, t.L(), c.WorkloadNode(), "haproxy"); err != nil {
			t.Fatal(err)
		}
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach gen haproxy --url {pgurl:1}")
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), "haproxy -f haproxy.cfg -D")
	}

	t.Status("installing sysbench")
	if err := c.Install(ctx, t.L(), c.WorkloadNode(), "sysbench"); err != nil {
		t.Fatal(err)
	}

	// Keep track of the start time for roachperf. Note that this is just an
	// estimate and not as accurate as what a workload histogram would give.
	var start time.Time
	runWorkload := func(ctx context.Context) error {
		t.Status("preparing workload")
		cmd := opts.cmd(useHAProxy /* haproxy */)
		{
			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()), roachtestutil.PrefixCmdOutputWithTimestamp(cmd+" prepare"))
			if err != nil {
				return err
			} else if msg, crashed := detectSysbenchCrash(result); crashed {
				t.Skipf("%s; skipping test", msg)
			} else if strings.Contains(result.Stdout, "FATAL") {
				// sysbench prepare doesn't exit on errors for some reason, so we have
				// to check that it didn't silently fail. We've seen it do so, causing
				// the run step to segfault. Segfaults are an ignored error, so in the
				// past, this would cause the test to silently fail.
				return errors.Newf("sysbench prepare failed with FATAL error")
			}
		}

		t.Status("warming up via oltp_read_only")
		{
			opts := opts
			opts.workload = oltpReadOnly
			opts.duration = 3 * time.Minute

			result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()),
				opts.cmd(useHAProxy)+" run")

			if msg, crashed := detectSysbenchCrash(result); crashed {
				t.L().Printf("%s; proceeding to main workload anyway", msg)
				err = nil
			}
			require.NoError(t, err)
		}

		t.Status("running workload")
		start = timeutil.Now()
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()), roachtestutil.PrefixCmdOutputWithTimestamp(cmd+" run"))

		if msg, crashed := detectSysbenchCrash(result); crashed {
			t.Skipf("%s; skipping test", msg)
		}

		if err != nil {
			return err
		}

		t.Status("exporting results")
		idx := strings.Index(result.Stdout, "SQL statistics:")
		if idx < 0 {
			return errors.Errorf("no SQL statistics found in sysbench output:\n%s", result.Stdout)
		}
		t.L().Printf("sysbench results:\n%s", result.Stdout[idx:])

		if err := exportSysbenchResults(t, c, result.Stdout, start, opts); err != nil {
			return err
		}

		// Also produce standard Go benchmark output. This can be used to run
		// benchstat comparisons.
		goBenchOutput, err := sysbenchToGoBench(t.Name(), result.Stdout[idx:])
		if err != nil {
			return err
		}
		// NB: 1.perf was created in exportSysbenchResults above.
		if err := os.WriteFile(filepath.Join(t.ArtifactsDir(), "1.perf", "bench.txt"), []byte(goBenchOutput),
			0666); err != nil {
			return err
		}

		// The remainder of this method collects profiles and applies only to CRDB,
		// so exit early if benchmarking postgres.
		if opts.usePostgres {
			return nil
		}

		t.Status("running 75 second workload to collect profiles")
		{
			// We store profiles in the perf directory. That way, they're not zipped
			// up and are available for retrieval for pgo updates more easily.
			profilesDir := filepath.Join(t.ArtifactsDir(), "1.perf", "profiles")
			require.NoError(t, os.MkdirAll(profilesDir, 0755))

			// Start a short sysbench test in order to collect the profiles from an
			// active cluster.
			m := t.NewErrorGroup(task.WithContext(ctx))
			m.Go(
				func(ctx context.Context, l *logger.Logger) error {
					opts := opts
					opts.duration = 75 * time.Second
					result, err = c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()),
						opts.cmd(useHAProxy)+" run")

					if msg, crashed := detectSysbenchCrash(result); crashed {
						t.L().Printf("%s; sysbench run to collect profiles failed", msg)
					}
					return err
				},
			)

			// Wait for 30 seconds to give a chance to the workload to start, and then
			// collect CPU, mutex diffs, allocs diffs profiles.
			time.Sleep(30 * time.Second)
			collectionDuration := 30 * time.Second

			// Collect the profiles.
			profiles := map[string][]*profile.Profile{"cpu": {}, "allocs": {}, "mutex": {}}
			for typ := range profiles {
				m.Go(
					func(ctx context.Context, l *logger.Logger) error {
						var err error
						profiles[typ], err = roachtestutil.GetProfile(ctx, t, c, typ,
							collectionDuration, c.CRDBNodes())
						return err
					},
				)
			}

			// If there is a problem executing the workload or there is a problem
			// collecting the profiles we need to clean up the directory and return
			// the error.
			if err := m.WaitE(); err != nil {
				require.NoError(t, os.RemoveAll(profilesDir))
				return err
			}

			// At this point we know that the workload has not crashed, and we have
			// collected all the individual profiles. We can now merge and export
			// them. If exporting or merging fails for some reason, we clean up the
			// profiles directory and return the error to avoid leaving potentially
			// corrupt profiles.
			if err := mergeAndExportSysbenchProfiles(c, collectionDuration, profiles,
				profilesDir); err != nil {
				require.NoError(t, os.RemoveAll(profilesDir))
				return err
			}
		}

		return nil
	}
	if opts.usePostgres {
		if err := runWorkload(ctx); err != nil {
			t.Fatal(err)
		}
	} else {
		m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
		m.Go(runWorkload)
		m.Wait()
	}
}

func registerSysbench(r registry.Registry) {
	coreThree := func(w sysbenchWorkload) bool {
		switch w {
		case oltpReadOnly, oltpReadWrite, oltpWriteOnly:
			return true
		default:
			return false
		}
	}

	for _, d := range []struct {
		n, cpus   int
		transform func(opts *sysbenchOptions) bool // false = skip
	}{
		{n: 1, cpus: 32},
		{n: 3, cpus: 32},
		{n: 3, cpus: 8,
			transform: func(opts *sysbenchOptions) bool {
				// Only run core three.
				return coreThree(opts.workload)
			},
		},
		{n: 3, cpus: 8,
			transform: func(opts *sysbenchOptions) bool {
				// Only run core three.
				if !coreThree(opts.workload) {
					return false
				}
				opts.extra = extraSetup{
					nameSuffix: "-settings",
					stmts: []string{
						`set cluster setting sql.stats.flush.enabled = false`,
						`set cluster setting sql.metrics.statement_details.enabled = false`,
						`set cluster setting kv.split_queue.enabled = false`,
						`set cluster setting kv.transaction.write_buffering.enabled = true`,
					},
					useDRPC: true,
				}
				return true
			},
		},
	} {
		for w := sysbenchWorkload(0); w < numSysbenchWorkloads; w++ {
			concPerCPU := d.n*3 - 1
			conc := d.cpus * concPerCPU
			opts := sysbenchOptions{
				workload:     w,
				duration:     10 * time.Minute,
				concurrency:  conc,
				tables:       10,
				rowsPerTable: 10000000,
			}
			if d.transform != nil && !d.transform(&opts) {
				continue
			}

			benchname := "sysbench"
			if opts.extra.nameSuffix != "" {
				benchname += opts.extra.nameSuffix
			}

			crdbSpec := registry.TestSpec{
				Name:                      fmt.Sprintf("%s/%s/nodes=%d/cpu=%d/conc=%d", benchname, w, d.n, d.cpus, conc),
				Benchmark:                 true,
				Owner:                     registry.OwnerTestEng,
				Cluster:                   r.MakeClusterSpec(d.n+1, spec.CPU(d.cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(16)),
				CompatibleClouds:          registry.OnlyGCE,
				Suites:                    registry.Suites(registry.Nightly),
				TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runSysbench(ctx, t, c, opts)
				},
			}
			r.Add(crdbSpec)

			// Add a variant of the single-node tests that uses PostgreSQL instead of CockroachDB.
			if d.n == 1 {
				pgOpts := opts
				pgOpts.usePostgres = true
				pgSpec := crdbSpec
				pgSpec.Name = fmt.Sprintf("%s/%s/postgres/cpu=%d/conc=%d", benchname, w, d.cpus, conc)
				pgSpec.Suites = registry.Suites(registry.Weekly)
				// Postgres installation creates a lot of directories not cleaned up by
				// cluster wipe. To avoid side effects on subsequent postgres sysbench
				// runs, don't reuse the cluster.
				pgSpec.Cluster.ReusePolicy = spec.ReusePolicyNone{}
				pgSpec.TestSelectionOptOutSuites = registry.Suites(registry.Weekly)
				pgSpec.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runSysbench(ctx, t, c, pgOpts)
				}
				r.Add(pgSpec)
			}
		}
	}
}

type sysbenchMetrics struct {
	Time         int64  `json:"time"`
	Threads      string `json:"threads"`
	Transactions string `json:"transactions"`
	Qps          string `json:"qps"`
	ReadQps      string `json:"readQps"`
	WriteQps     string `json:"writeQps"`
	OtherQps     string `json:"otherQps"`
	P95Latency   string `json:"p95Latency"`
	Errors       string `json:"errors"`
	Reconnects   string `json:"reconnects"`
}

type openmetricsValues struct {
	Value string
	Time  int64
}

// Define units for sysbench metrics
var units = map[string]string{
	// Transaction rates - operations per second
	"transactions": "unit=\"ops/sec\",is_higher_better=\"true\"",

	// Query rates - queries per second
	"qps":       "unit=\"qps\",is_higher_better=\"true\"",
	"read_qps":  "unit=\"qps\",is_higher_better=\"true\"",
	"write_qps": "unit=\"qps\",is_higher_better=\"true\"",
	"other_qps": "unit=\"qps\",is_higher_better=\"true\"",

	// Latency - milliseconds
	"p95_latency": "unit=\"ms\",is_higher_better=\"false\"",

	// Error rates - errors per second
	"errors": "unit=\"errors/sec\",is_higher_better=\"false\"",

	// Reconnection rates - reconnects per second
	"reconnects": "unit=\"reconnects/sec\",is_higher_better=\"false\"",
}

// exportSysbenchResults parses the output of `sysbench` into a stats
// file and writes it to the perf directory that roachperf expects. The
// format of the stats file is dependent on t.ExportOpenmetrics().
// Sysbench does have a way to customize the report output via injecting
// a custom `sysbench.hooks.report_intermediate` hook, but then we would
// lose the human-readable output in the test itself.
func exportSysbenchResults(
	t test.Test, c cluster.Cluster, result string, start time.Time, opts sysbenchOptions,
) error {
	// Parse the results into a JSON file that roachperf understands.
	// The output of the results look like:
	// 		1. Start up information.
	//		2. Benchmark metrics timeseries every second.
	//		3. Benchmark metrics summary.
	//
	// For roachperf, we care about 2, so filter out any line that
	// doesn't start with a timestamp.
	//
	// An example line of this output:
	// [ 1s ] thds: 256 tps: 2696.16 qps: 57806.17 (r/w/o: 40988.38/11147.98/5669.82) lat (ms,95%): 196.89 err/s: 21.96 reconn/s: 0.00

	filter := "\\[ [\\d]+s \\].*"
	regex, err := regexp.Compile(filter)
	if err != nil {
		return err
	}

	var metricBytes []byte

	var snapshotsFound int
	s := bufio.NewScanner(strings.NewReader(result))
	labels := map[string]string{
		"distribution":   opts.distribution,
		"duration":       fmt.Sprintf("%f", opts.duration.Seconds()),
		"concurrency":    fmt.Sprintf("%d", opts.concurrency),
		"table":          fmt.Sprintf("%d", opts.tables),
		"rows-per-table": fmt.Sprintf("%d", opts.rowsPerTable),
		"use-postgres":   fmt.Sprintf("%t", opts.usePostgres),
	}
	labelString := roachtestutil.GetOpenmetricsLabelString(t, c, labels)
	openmetricsMap := make(map[string][]openmetricsValues)

	// Counters for aggregated metrics
	var totalQpsSum, readQpsSum, writeQpsSum, otherQpsSum float64
	var sampleCount int64

	tick := func(fields []string, qpsByType []string) error {
		snapshotTick := sysbenchMetrics{
			Time:         start.Unix(),
			Threads:      fields[1],
			Transactions: fields[3],
			Qps:          fields[5],
			ReadQps:      qpsByType[0],
			WriteQps:     qpsByType[1],
			OtherQps:     qpsByType[2],
			P95Latency:   fields[10],
			Errors:       fields[12],
			Reconnects:   fields[14],
		}

		// Add to aggregation counters
		qpsVal, _ := strconv.ParseFloat(fields[5], 64)
		readQpsVal, _ := strconv.ParseFloat(qpsByType[0], 64)
		writeQpsVal, _ := strconv.ParseFloat(qpsByType[1], 64)
		otherQpsVal, _ := strconv.ParseFloat(qpsByType[2], 64)

		totalQpsSum += qpsVal
		readQpsSum += readQpsVal
		writeQpsSum += writeQpsVal
		otherQpsSum += otherQpsVal
		sampleCount++

		if t.ExportOpenmetrics() {
			addCurrentSnapshotToOpenmetrics(snapshotTick, openmetricsMap)
		} else {
			var snapshotTickBytes []byte
			snapshotTickBytes, err = json.Marshal(snapshotTick)
			if err != nil {
				return errors.Errorf("error marshaling metrics")
			}
			snapshotTickBytes = append(snapshotTickBytes, []byte("\n")...)
			metricBytes = append(metricBytes, snapshotTickBytes...)
		}

		start = start.Add(time.Second)
		return nil
	}

	for s.Scan() {
		if matched := regex.MatchString(s.Text()); !matched {
			continue
		}
		snapshotsFound++

		// Remove the timestamp to make subsequent parsing easier.
		_, output, _ := strings.Cut(s.Text(), "] ")
		fields := strings.Fields(output)
		if len(fields) != 15 {
			return errors.Errorf("metrics output in unexpected format, expected 15 fields got: %d", len(fields))
		}

		// Individual QPS is formatted like: (r/w/o: 40988.38/11147.98/5669.82),
		// so we need to handle it separately.
		qpsByType := strings.Split(strings.Trim(fields[7], "()"), "/")
		if len(qpsByType) != 3 {
			return errors.Errorf("QPS metrics output in unexpected format, expected 3 fields got: %d", len(qpsByType))
		}

		if err := tick(fields, qpsByType); err != nil {
			return err
		}

	}
	// Guard against the possibility that the format changed and we no longer
	// get any output.
	if snapshotsFound == 0 {
		return errors.Errorf("No lines started with expected format: %s", filter)
	}
	t.L().Printf("exportSysbenchResults: %d lines parsed", snapshotsFound)

	// Copy the metrics to the artifacts directory, so it can be exported to roachperf.
	// Assume single node artifacts, since the metrics we get are aggregated amongst the cluster.
	perfDir := filepath.Join(t.ArtifactsDir(), "/1.perf")
	if err := os.MkdirAll(perfDir, 0755); err != nil {
		return err
	}

	if t.ExportOpenmetrics() {
		metricBytes = getOpenmetricsBytes(openmetricsMap, labelString)
	}

	// Write the standard metrics file
	if err := os.WriteFile(fmt.Sprintf("%s/%s", perfDir, roachtestutil.GetBenchmarkMetricsFileName(t)), metricBytes, 0666); err != nil {
		return err
	}

	// If using OpenMetrics, also calculate and write aggregated metrics
	if t.ExportOpenmetrics() && sampleCount > 0 {
		floatSampleCount := float64(sampleCount)
		avgTotalQps := totalQpsSum / floatSampleCount
		avgReadQps := readQpsSum / floatSampleCount
		avgWriteQps := writeQpsSum / floatSampleCount
		avgOtherQps := otherQpsSum / floatSampleCount

		// Create aggregated metrics exactly matching roachperf's expected format
		aggregatedMetrics := roachtestutil.AggregatedPerfMetrics{
			{
				Name:           "total_qps",
				Value:          roachtestutil.MetricPoint(avgTotalQps),
				Unit:           "ops/s",
				IsHigherBetter: true,
			},
			{
				Name:           "read_qps",
				Value:          roachtestutil.MetricPoint(avgReadQps),
				Unit:           "ops/s",
				IsHigherBetter: true,
			},
			{
				Name:           "write_qps",
				Value:          roachtestutil.MetricPoint(avgWriteQps),
				Unit:           "ops/s",
				IsHigherBetter: true,
			},
			{
				Name:           "other_qps",
				Value:          roachtestutil.MetricPoint(avgOtherQps),
				Unit:           "ops/s",
				IsHigherBetter: true,
			},
		}

		aggregatedBuf := &bytes.Buffer{}

		labels, err := roachtestutil.GetLabels(labelString)
		if err != nil {
			return errors.Wrap(err, "failed to get labels")
		}
		// Convert aggregated metrics to OpenMetrics format
		if err := roachtestutil.GetAggregatedMetricBytes(
			aggregatedMetrics,
			labels,
			timeutil.Now(),
			aggregatedBuf,
		); err != nil {
			return errors.Wrap(err, "failed to format aggregated metrics")
		}

		// Write aggregated metrics
		aggregatedFileName := "aggregated_" + roachtestutil.GetBenchmarkMetricsFileName(t)
		aggregatedPath := filepath.Join(perfDir, aggregatedFileName)
		if err := os.WriteFile(aggregatedPath, aggregatedBuf.Bytes(), 0644); err != nil {
			return errors.Wrap(err, "failed to write aggregated metrics")
		}

		t.L().Printf("Wrote aggregated metrics to %s", aggregatedPath)
	}

	return nil
}

// mergeAndExportSysbenchProfiles accepts a map of individual profiles of each
// node of different types (cpu, allocs, mutex), and exports them to the
// specified directory. Also, it merges them and exports the merged profiles
// to the same directory.
func mergeAndExportSysbenchProfiles(
	c cluster.Cluster,
	duration time.Duration,
	profiles map[string][]*profile.Profile,
	profilesDir string,
) error {
	// Merge the profiles.
	mergedProfiles := map[string]*profile.Profile{"cpu": {}, "allocs": {}, "mutex": {}}
	for typ := range mergedProfiles {
		var err error
		if mergedProfiles[typ], err = profile.Merge(profiles[typ]); err != nil {
			return errors.Wrapf(err, "failed to merge profiles type: %s", typ)
		}
	}

	// Export the merged profiles.
	for typ := range mergedProfiles {
		if err := roachtestutil.ExportProfile(mergedProfiles[typ], profilesDir,
			fmt.Sprintf("merged.%s.pb.gz", typ)); err != nil {
			return errors.Wrapf(err, "failed to export merged profiles: %s", typ)
		}
	}

	// Export the individual profiles as well.
	for i := range len(c.CRDBNodes()) {
		for typ := range profiles {
			if err := roachtestutil.ExportProfile(profiles[typ][i], profilesDir,
				fmt.Sprintf("n%d.%s%s.pb.gz", i+1, typ, duration)); err != nil {
				return errors.Wrapf(err, "failed to export individual profile type: %s", typ)
			}
		}
	}

	return nil
}

// Add sysbenchMetrics to the openmetricsMap
func addCurrentSnapshotToOpenmetrics(
	metrics sysbenchMetrics, openmetricsMap map[string][]openmetricsValues,
) {
	time := metrics.Time
	openmetricsMap["transactions"] = append(openmetricsMap["transactions"], openmetricsValues{Value: metrics.Transactions, Time: time})
	openmetricsMap["qps"] = append(openmetricsMap["qps"], openmetricsValues{Value: metrics.Qps, Time: time})
	openmetricsMap["read_qps"] = append(openmetricsMap["read_qps"], openmetricsValues{Value: metrics.ReadQps, Time: time})
	openmetricsMap["write_qps"] = append(openmetricsMap["write_qps"], openmetricsValues{Value: metrics.WriteQps, Time: time})
	openmetricsMap["other_qps"] = append(openmetricsMap["other_qps"], openmetricsValues{Value: metrics.OtherQps, Time: time})
	openmetricsMap["p95_latency"] = append(openmetricsMap["p95_latency"], openmetricsValues{Value: metrics.P95Latency, Time: time})
	openmetricsMap["errors"] = append(openmetricsMap["errors"], openmetricsValues{Value: metrics.Errors, Time: time})
	openmetricsMap["reconnects"] = append(openmetricsMap["reconnects"], openmetricsValues{Value: metrics.Reconnects, Time: time})
}

// Convert openmetricsMap to bytes for writing to file
func getOpenmetricsBytes(openmetricsMap map[string][]openmetricsValues, labelString string) []byte {
	metricsBuf := bytes.NewBuffer([]byte{})
	for key, values := range openmetricsMap {
		metricName := util.SanitizeMetricName(key)
		metricsBuf.WriteString(roachtestutil.GetOpenmetricsGaugeType(metricName))
		for _, value := range values {
			metricsBuf.WriteString(fmt.Sprintf("%s{%s,%s} %s %d\n",
				metricName,
				labelString,
				units[key],
				value.Value,
				value.Time))
		}
	}

	// Add # EOF at the end for openmetrics
	metricsBuf.WriteString("# EOF\n")
	return metricsBuf.Bytes()
}

func detectSysbenchCrash(result install.RunResultDetails) (string, bool) {
	// Sysbench occasionally segfaults. When that happens, don't fail the
	// test.
	if result.RemoteExitStatus == roachprodErrors.SegmentationFaultExitCode {
		return "sysbench segfaulted", true
	} else if result.RemoteExitStatus == roachprodErrors.IllegalInstructionExitCode {
		return "sysbench crashed with illegal instruction", true
	} else if result.RemoteExitStatus == roachprodErrors.AssertionFailureExitCode {
		return "sysbench crashed with an assertion failure", true
	}
	return "", false
}

// sysbenchToGoBench converts sysbench output into Go benchmark format.
func sysbenchToGoBench(name string, result string) (string, error) {
	// Extract key metrics from sysbench output using regex patterns.
	var qps, tps string
	var minLat, avgLat, p95Lat, maxLat string

	// Parse transactions per second.
	m := regexp.MustCompile(`transactions:\s+\d+\s+\(([\d.]+)\s+per sec`).FindStringSubmatch(result)
	if len(m) <= 1 {
		return "", errors.New("failed to parse transactions per second")
	}
	tps = m[1]

	// Parse queries per second.
	m = regexp.MustCompile(`queries:\s+\d+\s+\(([\d.]+)\s+per sec`).FindStringSubmatch(result)
	if len(m) <= 1 {
		return "", errors.New("failed to parse queries per second")
	}
	qps = m[1]

	// Parse each latency metric using a loop.
	metrics := map[string]*string{
		"min":             &minLat,
		"avg":             &avgLat,
		"max":             &maxLat,
		"95th percentile": &p95Lat,
	}
	for metric, ptr := range metrics {
		pattern := fmt.Sprintf(`%s:\s+([\d.]+)`, metric)
		m = regexp.MustCompile(pattern).FindStringSubmatch(result)
		if len(m) <= 1 {
			return "", errors.Newf("failed to parse %s latency", metric)
		}
		*ptr = m[1]
	}

	// Process the test name.
	parts := strings.Split(name, "/")
	if len(parts) == 0 {
		return "", errors.New("empty test name")
	}

	// Normalize first segment (e.g. "sysbench-settings" -> "SysbenchSettings").
	firstPart := parts[0]
	// Split on non-alphanumeric characters.
	words := regexp.MustCompile(`[^a-zA-Z0-9]+`).Split(firstPart, -1)
	// Capitalize each word and join them.
	var sb strings.Builder
	for _, word := range words {
		if word == "" {
			continue
		}
		sb.WriteString(cases.Title(language.Und).String(strings.ToLower(word)))
	}
	firstPart = sb.String()

	// Build the benchmark name.
	benchName := "Benchmark" + firstPart

	// Add remaining parts, using auto-assigned keys only for parts without keys.
	nextKey := 'a'
	for _, part := range parts[1:] {
		if strings.Contains(part, "=") {
			benchName += "/" + part
		} else {
			benchName += fmt.Sprintf("/%s=%s", string(nextKey), part)
			nextKey++
		}
	}

	// Return formatted benchmark string with all metrics.
	return fmt.Sprintf("%s\t1\t%s queries/sec\t%s txns/sec\t%s ms/min\t%s ms/avg\t%s ms/p95\t%s ms/max\n",
		benchName, qps, tps, minLat, avgLat, p95Lat, maxLat), nil
}
