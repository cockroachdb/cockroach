// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sniffarg"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// BenchmarkRaftStore runs the experiments in the testdata directory. It sets up
// a synthetic raft/state machine workload (according to the specific testdata
// configuration being run) and collects metrics on it, especially related to
// write amplification.
//
// The testdata files support only two commands: "run" and "disabled". The
// former runs the benchmark with the given configuration, and the latter skips
// the benchmark (running it only in "smoke check" mode through
// TestBenchmarkRaftStoreSmokeTest). This makes it easy to run custom sets of
// experiments without having to pass complicated regular expressions to `go
// test -bench`.
// In either case, the input to the command is a yaml document describing a
// subset of a Config struct. The benchmark then merges these with the default
// configuration and runs the workload. The output of the test case is a
// yaml-encoded representation of the Config that was run.
//
// BenchmarkRaftStore is not a traditional Go benchmark in that it does not
// respect the `b.N` parameter. Instead, the number of operations is hard-coded
// in the testdata file. Relying on the Go benchmark harness is useful because
// it allows for easy comparison of results between different configurations
// using benchstat.
//
// Due to the above, BenchmarkRaftStore skips itself unless invoked with
// test.benchtime=1x. This implies that it is not exercised during CI or nightly
// benchmark runs - an intentional choice, since the full suite takes many hours
// to run and requires hundreds of GBs of disk space.
func BenchmarkRaftStore(t *testing.B) {
	defer leaktest.AfterTest(t)()
	var benchTime string
	require.NoError(t, sniffarg.DoEnv("test.benchtime", &benchTime))
	if benchTime != "1x" {
		skip.IgnoreLintf(t, "not a traditional benchmark; use -test.benchtime=1x to run full suite")
	}

	require.False(t, metamorphic.IsMetamorphicBuild(),
		"metamorphic testing is enabled: disable via COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true")
	// Pebble needs at least a few goroutines to run compactions.
	require.Greater(t, runtime.GOMAXPROCS(0), 8, "GOMAXPROCS must be > 8")

	benchmarkRaftStoreInner(t, false)
}

func benchmarkRaftStoreInner(t testing.TB, smokeCheckOnly bool) {
	tdPath := datapathutils.TestDataPath(t)
	datadriven.WalkAny(t, tdPath, func(t testing.TB, path string) {
		datadriven.RunTestAny(t, path, func(t testing.TB, d *datadriven.TestData) string {
			scope := log.Scope(t)
			t.Cleanup(func() { scope.Close(t) }) // plays nice with pebble engine lifecycle
			switch d.Cmd {
			case "run":
			case "disabled":
				if !smokeCheckOnly {
					skip.IgnoreLintf(t, "skipping test due to `disabled` directive in testdata file")
				}
				// If smoke checking, still run the config to make sure a) it doesn't
				// fail and b) the testdata file is up to date.
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			dec := yaml.NewDecoder(strings.NewReader(d.Input))
			dec.SetStrict(true)
			cfg := MakeDefaultConfig()
			require.NoError(t, dec.Decode(&cfg))

			// TODO(tbg): could support fixtures, using:
			// testfixtures.ReuseOrGenerate(b, name, func(dir string) {})

			cfg.Dir = determineOutputDir(t)

			goldenCfg := cfg
			if smokeCheckOnly {
				logf(t, "smoke check only: overriding config with minimal dataset")
				cfg.NumReplicas = 1
				cfg.NumWorkers = 1
				cfg.NumWrites = 1
			}

			logf(t, "running config:\n%s", configToYAML(t, cfg))
			res := Run(t, cfg)
			require.NoError(t, os.WriteFile(
				filepath.Join(cfg.Dir, "config.yml"), []byte(configToYAML(t, cfg)), 0644),
			)
			require.NoError(t, os.WriteFile(
				filepath.Join(cfg.Dir, "result.yml"), []byte(yamlEncode(t, res)), 0644),
			)

			maybeReportMetrics(t, res)

			cfg = goldenCfg // see `if !smokeCheckOnly` above
			return configToYAML(t, stripConfig(cfg))
		})
	})
}

func determineOutputDir(t testing.TB) string {
	var testOutDir string
	require.NoError(t, sniffarg.DoEnv("test.outputdir", &testOutDir))
	if testOutDir == "" {
		return t.TempDir()
	}
	return filepath.Join(testOutDir, t.Name())
}

func stripConfig(cfg Config) Config {
	// Suppress some fields in the output. These don't change the
	// results.
	cfg.Dir = ""
	cfg.WALMetrics = false
	cfg.RaftMetricsInterval = 0
	if cfg.SingleEngine {
		cfg.RaftL0Threshold = 0
		cfg.RaftMemtableBytes = 0
	}
	return cfg
}

func configToYAML(t T, cfg Config) string {
	var buf bytes.Buffer
	require.NoError(t, yaml.NewEncoder(&buf).Encode(&cfg))
	return buf.String()
}

func maybeReportMetrics(t T, res Result) {
	b, ok := t.(*testing.B)
	if !ok {
		return // skip if we are not running a benchmark
	}
	b.ReportMetric(res.PayloadGB, "payloadGB")
	b.ReportMetric(res.GoodputMB, "MB/sec")
	b.ReportMetric(float64(res.Truncations), "truncs")
	b.ReportMetric(res.CombinedEng.WriteAmp, "wamp")
	b.ReportMetric(res.CombinedEng.FlushFrac, "flushFrac")
	b.ReportMetric(res.CombinedEng.WALGB, "walGB")
	b.ReportMetric(res.CombinedEng.CompactionGB, "compactionGB")
	b.ReportMetric(res.CombinedEng.FlushGB, "flushGB")
}

// TestBenchmarkRaftStoreSmokeTest exercises BenchmarkRaftStore with a trivial
// amount of data. This is useful because BenchmarkRaftStore is not exercised by
// nightly benchmark runs or CI (since it lightly abuses Go benchmark infra and
// is too heavyweight).
func TestBenchmarkRaftStoreSmokeTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	benchmarkRaftStoreInner(t, true)
}
