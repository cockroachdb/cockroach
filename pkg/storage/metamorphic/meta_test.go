// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metamorphic

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

var (
	keep         = flag.Bool("keep", false, "keep temp directories after test")
	check        = flag.String("check", "", "run operations in specified file and check output for equality")
	inMem        = flag.Bool("in-mem", false, "use an in-memory filesystem")
	compareFiles = flag.String("compare-files", "", "comma-separated list of output files to compare; used by TestCompareFiles")
	opCount      = flag.Int("operations", 10000, "number of MVCC operations to generate and run")
)

type testRun struct {
	ctx             context.Context
	t               *testing.T
	seed            int64
	inMem           bool
	checkFile       string
	restarts        bool
	engineSequences []engineSequence
}

type testRunForEngines struct {
	ctx            context.Context
	t              *testing.T
	inMem          bool
	seed           int64
	restarts       bool
	checkFile      io.Reader
	outputFile     io.Writer
	engineSequence engineSequence
}

func runMetaTestForEngines(run testRunForEngines) {
	tempDir, cleanup := testutils.TempDir(run.t)
	defer func() {
		if !*keep && !run.t.Failed() {
			cleanup()
		}
	}()
	var fs vfs.FS
	if run.inMem && !*keep {
		fs = vfs.NewMem()
	} else {
		fs = vfs.Default
	}

	testRunner := metaTestRunner{
		ctx:       run.ctx,
		t:         run.t,
		w:         run.outputFile,
		engineFS:  fs,
		seed:      run.seed,
		restarts:  run.restarts,
		engineSeq: run.engineSequence,
		path:      filepath.Join(tempDir, "store"),
		// TODO(travers): Add metamorphic test support for different versions, which
		// will give us better coverage across multiple format major versions and
		// table versions.
		st: cluster.MakeTestingClusterSettings(),
	}
	fmt.Printf("store path = %s\n", testRunner.path)

	testRunner.init()
	defer testRunner.closeAll()
	if run.checkFile != nil {
		testRunner.parseFileAndRun(run.checkFile)
	} else {
		testRunner.generateAndRun(*opCount)
	}
}

func runMetaTest(run testRun) {
	t := run.t
	outerTempDir, cleanup := testutils.TempDir(run.t)
	defer func() {
		if !*keep && !t.Failed() {
			cleanup()
		}
	}()

	// The test run with the first engine sequence writes its output to this file.
	// All subsequent engine sequence runs compare their output against this file.
	firstRunOutput := filepath.Join(outerTempDir, "output.meta")
	firstRunExecuted := false
	fmt.Printf("first run output file: %s\n", firstRunOutput)

	for _, engineSequence := range run.engineSequences {

		t.Run(fmt.Sprintf("engine/%s", engineSequence.name), func(t *testing.T) {
			innerTempDir, cleanup := testutils.TempDir(t)
			defer func() {
				if !*keep && !t.Failed() {
					cleanup()
				}
			}()

			// If this is not the first sequence run and a "check" file was not passed
			// in, use the first run's output file as the check file.
			var checkFileReader io.ReadCloser
			if run.checkFile == "" && firstRunExecuted {
				run.checkFile = firstRunOutput
			}
			if run.checkFile != "" {
				var err error
				checkFileReader, err = os.Open(run.checkFile)
				if err != nil {
					t.Fatal(err)
				}
				defer checkFileReader.Close()
			}

			var outputFileWriter io.WriteCloser
			outputFile := firstRunOutput
			if firstRunExecuted {
				outputFile = filepath.Join(innerTempDir, "output.meta")
			}
			var err error
			outputFileWriter, err = os.Create(outputFile)
			if err != nil {
				t.Fatal(err)
			}
			defer outputFileWriter.Close()
			fmt.Printf("check file = %s\noutput file = %s\n", run.checkFile, outputFile)
			engineRun := testRunForEngines{
				ctx:            run.ctx,
				t:              t,
				inMem:          run.inMem,
				seed:           run.seed,
				restarts:       run.restarts,
				checkFile:      checkFileReader,
				outputFile:     outputFileWriter,
				engineSequence: engineSequence,
			}
			runMetaTestForEngines(engineRun)
			firstRunExecuted = true
		})
	}
}

// TestPebbleEquivalence runs the MVCC Metamorphic test suite, and checks
// for matching outputs by the test suite between different options of Pebble.
func TestPebbleEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)
	runPebbleEquivalenceTest(t)
}

func runPebbleEquivalenceTest(t *testing.T) {
	ctx := context.Background()
	// This test times out with the race detector enabled.
	_, seed := randutil.NewTestRand()

	engineSeqs := make([]engineSequence, 0, numStandardOptions+numRandomOptions)

	for i := 0; i < numStandardOptions; i++ {
		engineSeq := engineSequence{
			configs: []engineConfig{{
				name: fmt.Sprintf("standard=%d", i),
				opts: standardOptions(i),
			}},
		}
		engineSeq.name = engineSeq.configs[0].name
		engineSeqs = append(engineSeqs, engineSeq)
	}

	for i := 0; i < numRandomOptions; i++ {
		engineSeq := engineSequence{
			configs: []engineConfig{{
				name: fmt.Sprintf("random=%d", i),
				opts: randomOptions(),
			}},
		}
		engineSeq.name = engineSeq.configs[0].name
		engineSeqs = append(engineSeqs, engineSeq)
	}

	run := testRun{
		ctx:             ctx,
		t:               t,
		seed:            seed,
		restarts:        false,
		inMem:           true,
		engineSequences: engineSeqs,
	}
	runMetaTest(run)
}

// TestPebbleRestarts runs the MVCC Metamorphic test suite with restarts
// enabled, and ensures that the output remains the same across different
// engine sequences with restarts in between.
func TestPebbleRestarts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test times out with the race detector enabled.
	skip.UnderRace(t)
	_, seed := randutil.NewTestRand()

	engineConfigs := make([]engineConfig, 0, numStandardOptions+numRandomOptions-1)
	// Create one config sequence that contains all options.
	for i := 0; i < numStandardOptions; i++ {
		// Skip standard config at index 9 as it's incompatible with restarts.
		if i == 9 {
			continue
		}
		engineConfigs = append(engineConfigs, engineConfig{
			name: fmt.Sprintf("standard=%d", i),
			opts: standardOptions(i),
		})
	}

	for i := 0; i < numRandomOptions; i++ {
		engineConfigs = append(engineConfigs, engineConfig{
			name: fmt.Sprintf("random=%d", i),
			opts: randomOptions(),
		})
	}

	ctx := context.Background()
	run := testRun{
		ctx:      ctx,
		t:        t,
		seed:     seed,
		inMem:    true,
		restarts: true,
		engineSequences: []engineSequence{
			{name: "standard", configs: []engineConfig{engineConfigStandard}},
			{
				name:    fmt.Sprintf("standards=%d,randoms=%d", numStandardOptions-1, numRandomOptions),
				configs: engineConfigs,
			},
		},
	}
	runMetaTest(run)
}

// TestPebbleCheck checks whether the output file specified with --check has
// matching behavior with a standard run of pebble.
func TestPebbleCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	if *check == "" {
		skip.IgnoreLint(t, "Skipping; no check file provided via --check")
		return
	}
	if _, err := os.Stat(*check); oserror.IsNotExist(err) {
		t.Fatal(err)
	}

	engineSeqs := make([]engineSequence, 0, numStandardOptions+numRandomOptions)

	for i := 0; i < numStandardOptions; i++ {
		engineSeq := engineSequence{
			configs: []engineConfig{{
				name: fmt.Sprintf("standard=%d", i),
				opts: standardOptions(i),
			}},
		}
		engineSeq.name = engineSeq.configs[0].name
		engineSeqs = append(engineSeqs, engineSeq)
	}

	for i := 0; i < numRandomOptions; i++ {
		engineSeq := engineSequence{
			configs: []engineConfig{{
				name: fmt.Sprintf("random=%d", i),
				opts: randomOptions(),
			}},
		}
		engineSeq.name = engineSeq.configs[0].name
		engineSeqs = append(engineSeqs, engineSeq)
	}

	run := testRun{
		ctx:             ctx,
		t:               t,
		checkFile:       *check,
		restarts:        true,
		inMem:           *inMem,
		engineSequences: engineSeqs,
	}
	runMetaTest(run)
}

// TestCompareFiles takes a comma-separated list of output files through the
// `--compare-files` command-line parameter. The output files should originate
// from the same run and have matching operations. TestRunCompare takes the
// operations from the provided `--check` file, and runs all the compare-files
// configurations against the operations, checking for equality.
//
// For example, suppose a nightly discovers a metamorphic failure where the
// random-008 run diverges. You can download 'output.meta', the first run with
// the standard options, and output file for the random run. Pass the
// output.meta to `--check` and the diverging run's output.meta to
// `--compare-files`:
//
//	./dev test -v ./pkg/storage/metamorphic -f TestCompareFiles --ignore-cache \
//	  --test-args '--in-mem' \
//	  --test-args '--check=/Users/craig/archive/output.meta' \
//	  --test-args '--compare-files=/Users/craig/archive/random8.meta'
//
// The above example supplies `--in-mem`. This may be useful to produce quick
// reproductions, but if you want to dig through the data directory, omit it.
func TestCompareFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	if *check == "" {
		skip.IgnoreLint(t, "Skipping; no check file provided via --check")
		return
	}
	if *compareFiles == "" {
		skip.IgnoreLint(t, "Skipping; no files to compare provided via --compare-files")
		return
	}

	// Check that all the referenced files exist.
	if _, err := os.Stat(*check); oserror.IsNotExist(err) {
		t.Fatal(err)
	}
	files := strings.Split(*compareFiles, ",")
	for _, f := range files {
		if _, err := os.Stat(f); oserror.IsNotExist(err) {
			t.Fatal(err)
		}
	}

	engineSeqs := make([]engineSequence, 0, len(files))
	for _, f := range files {
		cfg, seed, err := func() (engineConfig, int64, error) {
			r, err := os.Open(f)
			if err != nil {
				return engineConfig{}, 0, err
			}
			defer r.Close()
			return parseOutputPreamble(r)
		}()
		if err != nil {
			t.Fatalf("parsing file %q: %s", f, err)
		}
		engineSeqs = append(engineSeqs, engineSequence{
			name:    fmt.Sprintf("%s_%d", filepath.Base(f), seed),
			configs: []engineConfig{cfg},
		})
	}

	run := testRun{
		ctx:             ctx,
		t:               t,
		checkFile:       *check,
		restarts:        true,
		inMem:           *inMem,
		engineSequences: engineSeqs,
	}
	runMetaTest(run)
}
