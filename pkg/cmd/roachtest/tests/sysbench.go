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
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	roachprodErrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()), cmd+" prepare")
		if err != nil {
			return err
		} else if strings.Contains(result.Stdout, "FATAL") {
			// sysbench prepare doesn't exit on errors for some reason, so we have
			// to check that it didn't silently fail. We've seen it do so, causing
			// the run step to segfault. Segfaults are an ignored error, so in the
			// past, this would cause the test to silently fail.
			return errors.Newf("sysbench prepare failed with FATAL error")
		}

		t.Status("running workload")
		start = timeutil.Now()
		result, err = c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()), cmd+" run")

		// Sysbench occasionally segfaults. When that happens, don't fail the
		// test.
		if result.RemoteExitStatus == roachprodErrors.SegmentationFaultExitCode {
			t.L().Printf("sysbench segfaulted; passing test anyway")
			return nil
		} else if result.RemoteExitStatus == roachprodErrors.IllegalInstructionExitCode {
			t.L().Printf("sysbench crashed with illegal instruction; passing test anyway")
			return nil
		} else if result.RemoteExitStatus == roachprodErrors.AssertionFailureExitCode {
			t.L().Printf("sysbench crashed with an assertion failure; passing test anyway")
			return nil
		}

		if err != nil {
			return err
		}

		t.Status("exporting results")
		return exportSysbenchResults(t, c, result.Stdout, start, opts)
	}
	if opts.usePostgres {
		if err := runWorkload(ctx); err != nil {
			t.Fatal(err)
		}
	} else {
		m := c.NewMonitor(ctx, c.CRDBNodes())
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
		n, cpus int
		pick    func(sysbenchWorkload) bool // nil means true for all
		extra   extraSetup
	}{
		{n: 1, cpus: 32},
		{n: 3, cpus: 32},
		{n: 3, cpus: 8, pick: coreThree},
		{n: 3, cpus: 8, pick: coreThree,
			extra: extraSetup{
				nameSuffix: "-settings",
				stmts: []string{
					`set cluster setting sql.stats.flush.enabled = false`,
					`set cluster setting sql.metrics.statement_details.enabled = false`,
					`set cluster setting kv.split_queue.enabled = false`,
					`set cluster setting sql.stats.histogram_collection.enabled = false`,
					`set cluster setting kv.consistency_queue.enabled = false`,
				},
				useDRPC: true,
			},
		},
	} {
		for w := sysbenchWorkload(0); w < numSysbenchWorkloads; w++ {
			if d.pick != nil && !d.pick(w) {
				continue
			}
			concPerCPU := d.n*3 - 1
			conc := d.cpus * concPerCPU
			opts := sysbenchOptions{
				workload:     w,
				duration:     10 * time.Minute,
				concurrency:  conc,
				tables:       10,
				rowsPerTable: 10000000,
				extra:        d.extra,
			}

			benchname := "sysbench"
			if d.extra.nameSuffix != "" {
				benchname += d.extra.nameSuffix
			}

			r.Add(registry.TestSpec{
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
			})

			// Add a variant of each test that uses PostgreSQL instead of CockroachDB.
			if d.n == 1 {
				pgOpts := opts
				pgOpts.usePostgres = true
				r.Add(registry.TestSpec{
					Name:             fmt.Sprintf("sysbench/%s/postgres/cpu=%d/conc=%d", w, d.cpus, conc),
					Benchmark:        true,
					Owner:            registry.OwnerTestEng,
					Cluster:          r.MakeClusterSpec(d.n+1, spec.CPU(d.cpus), spec.WorkloadNode(), spec.WorkloadNodeCPU(16)),
					CompatibleClouds: registry.OnlyGCE,
					Suites:           registry.ManualOnly,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runSysbench(ctx, t, c, pgOpts)
					},
				})
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
	perfDir := fmt.Sprintf("%s/1.perf", t.ArtifactsDir())
	if err := os.MkdirAll(perfDir, 0755); err != nil {
		return err
	}

	if t.ExportOpenmetrics() {
		metricBytes = getOpenmetricsBytes(openmetricsMap, labelString)
	}
	return os.WriteFile(fmt.Sprintf("%s/%s", perfDir, roachtestutil.GetBenchmarkMetricsFileName(t)), metricBytes, 0666)
}

// Add sysbenchMetrics to the openmetricsMap
func addCurrentSnapshotToOpenmetrics(
	metrics sysbenchMetrics, openmetricsMap map[string][]openmetricsValues,
) {
	time := metrics.Time
	openmetricsMap["threads"] = append(openmetricsMap["threads"], openmetricsValues{Value: metrics.Threads, Time: time})
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
			metricsBuf.WriteString(fmt.Sprintf("%s{%s} %s %d\n", metricName, labelString, value.Value, value.Time))
		}
	}

	// Add # EOF at the end for openmetrics
	metricsBuf.WriteString("# EOF\n")
	return metricsBuf.Bytes()
}
