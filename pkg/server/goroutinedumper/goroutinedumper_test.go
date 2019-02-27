package goroutinedumper

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type goroutinesVal struct {
	secs       time.Duration // the time at which this goroutines value was emitted
	goroutines int64
}

func TestHeuristic(t *testing.T) {
	const dumpDir = "dump_dir"
	cases := []struct {
		name          string
		heuristics    []heuristic
		vals          []goroutinesVal
		dumpsToFail   []string
		expectedDumps []string
	}{
		{
			name:       "Use only numExceedThresholdHeuristic",
			heuristics: []heuristic{numExceedThresholdHeuristic},
			vals: []goroutinesVal{
				{0, 30},    // not trigger since smaller than threshold
				{10, 40},   // not trigger since smaller than threshold
				{20, 120},  // trigger
				{50, 35},   // not trigger since smaller than threshold
				{70, 150},  // not trigger since last dump was only 50 seconds ago
				{80, 130},  // trigger since last dump was 60 seconds ago
				{100, 135}, // not trigger as continues above threshold
				{180, 30},  // not trigger since smaller than threshold
				{190, 80},  // not trigger though it has doubled since last dump
				{220, 115}, // trigger since last dump was more than 60 seconds ago
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.num_exceed_threshold.000000120",
				"goroutine_dump.2019-01-01T00_01_20.num_exceed_threshold.000000130",
				"goroutine_dump.2019-01-01T00_03_40.num_exceed_threshold.000000115",
			},
		},
		{
			name:       "Use only growTooFastSinceLastCheckHeuristic",
			heuristics: []heuristic{growTooFastSinceLastCheckHeuristic},
			vals: []goroutinesVal{
				{0, 10},    // not trigger since smaller than lower limit
				{10, 15},   // not trigger since smaller than lower limit
				{20, 50},   // trigger since it has doubled
				{50, 35},   // not trigger since not doubled
				{70, 80},   // not trigger since last dump was only 50 seconds ago
				{80, 180},  // trigger since last dump was 60 seconds ago
				{100, 380}, // not trigger as continues to double
				{180, 250}, // not trigger since not doubled
				{190, 200}, // not trigger though it exceeds numGoroutinesThreshold
				{220, 500}, // trigger since last dump was more than 60 seconds ago
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.grow_too_fast_since_last_check.000000050",
				"goroutine_dump.2019-01-01T00_01_20.grow_too_fast_since_last_check.000000180",
				"goroutine_dump.2019-01-01T00_03_40.grow_too_fast_since_last_check.000000500",
			},
		},
		{
			name: "Use numExceedThreshold and growTooFastSinceLastCheck",
			heuristics: []heuristic{
				numExceedThresholdHeuristic,
				growTooFastSinceLastCheckHeuristic,
			},
			vals: []goroutinesVal{
				{0, 10},    // not trigger since smaller than lower limit
				{10, 15},   // not trigger since smaller than lower limit
				{20, 50},   // trigger since it has doubled
				{50, 35},   // not trigger since not doubled and below numGoroutinesThreshold
				{70, 80},   // not trigger since last dump was only 50 seconds ago
				{80, 150},  // trigger since last dump was 60 seconds ago
				{100, 90},  // not trigger since no heuristic is true
				{180, 120}, // trigger since it exceeds numGoroutinesThreshold
				{190, 200}, // not trigger since heuristic continues to be true
				{220, 500}, // not trigger since heuristic continues to be true
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.grow_too_fast_since_last_check.000000050",
				"goroutine_dump.2019-01-01T00_01_20.num_exceed_threshold.000000150",
				"goroutine_dump.2019-01-01T00_03_00.num_exceed_threshold.000000120",
			},
		},
		{
			name: "Fail some dumps when both heuristics are used",
			heuristics: []heuristic{
				numExceedThresholdHeuristic,
				growTooFastSinceLastCheckHeuristic,
			},
			vals: []goroutinesVal{
				{0, 10},    // not trigger since smaller than lower limit
				{10, 15},   // not trigger since smaller than lower limit
				{20, 50},   // trigger since it has doubled
				{50, 50},   // not trigger since no heuristic is true
				{70, 70},   // not trigger since no heuristic is true
				{80, 130},  // trigger but dump will fail
				{100, 120}, // trigger though goroutines exceed threshold in last check
				{180, 85},  // not trigger since no heuristic is true
				{200, 130}, // trigger since it exceeds numGoroutinesThreshold
				{220, 500}, // not trigger since heuristic continues to be true
			},
			expectedDumps: []string{
				"goroutine_dump.2019-01-01T00_00_20.grow_too_fast_since_last_check.000000050",
				"goroutine_dump.2019-01-01T00_01_40.num_exceed_threshold.000000120",
				"goroutine_dump.2019-01-01T00_03_20.num_exceed_threshold.000000130",
			},
			dumpsToFail: []string{
				"goroutine_dump.2019-01-01T00_01_20.num_exceed_threshold.000000130",
			},
		},
		{
			name: "No heuristic is used",
			heuristics: []heuristic{},
			vals: []goroutinesVal{
				{0, 10},
				{10, 15},
				{20, 50},
				{50, 35},
				{70, 80},
				{80, 150},
				{100, 120},
				{180, 85},
				{200, 130},
				{220, 500},
			},
			expectedDumps: nil,
		},
	}

	st := &cluster.Settings{}
	numGoroutinesThreshold.Override(&st.SV, 100)
	growthRateThreshold.Override(&st.SV, 2.0)
	lowerLimitForNumGoroutines.Override(&st.SV, 20)

	for _, c := range cases {
		baseTime := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
		var dumps []string
		var currentTime time.Time
		gd := GoroutineDumper{
			heuristics: c.heuristics,
			currentTime: func() time.Time {
				return currentTime
			},
			lastDumpTime: time.Time{},
			takeGoroutineDump: func(dir string, filename string) error {
				assert.Equal(t, dumpDir, dir)
				for _, d := range c.dumpsToFail {
					if filename == d {
						return errors.New("this dump is set to fail")
					}
				}
				dumps = append(dumps, filename)
				return nil
			},
			gc:  func(ctx context.Context, dir string, sizeLimit int64) {},
			dir: dumpDir,
		}

		ctx := context.TODO()
		for _, v := range c.vals {
			currentTime = baseTime.Add(v.secs * time.Second)
			gd.MaybeDump(ctx, st, v.goroutines)
		}
		assert.Equal(t, c.expectedDumps, dumps, "case '%s' failed", c.name)
	}
}

func TestNewGoroutineDumper(t *testing.T) {
	// NewGoroutineDumper fails because no directory specified
	_, err := NewGoroutineDumper("")
	assert.EqualError(t, err, "directory to store dumps could not be determined")

	// NewGoroutineDumper fails because goroutine_dump already exists as a file
	dir := os.TempDir()
	path := filepath.Join(dir, "goroutine_dump")
	func() {
		emptyFile, err := os.Create(path)
		assert.NoError(t, err, "failed to create goroutine_dump file")
		err = emptyFile.Close()
		assert.NoError(t, err, "failed to close goroutine_dump file")
		defer os.Remove(path)

		_, err = NewGoroutineDumper(dir)
		assert.EqualError(t, err, "mkdir /tmp/goroutine_dump: not a directory")
	}()

	// NewGoroutineDumper succeeds
	gd, err := NewGoroutineDumper(dir)
	defer os.Remove(path)
	assert.NoError(t, err, "unexpected error in NewGoroutineDumper")
	assert.Equal(t, timeutil.UnixEpoch, gd.lastDumpTime)
	assert.Equal(t, path, gd.dir)
}

func TestGc(t *testing.T) {
	type file struct {
		name string
		size int64
	}
	cases := []struct {
		name      string
		files     []file
		sizeLimit int64
		expected  []string
	}{
		{
			name: "total size smaller than size limit",
			files: []file{
				{name: "goroutine_dump.2019-01-01T00_00_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_10_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_20_00", size: 1},
			},
			sizeLimit: 5,
			expected: []string{
				"goroutine_dump.2019-01-01T00_00_00",
				"goroutine_dump.2019-01-01T00_10_00",
				"goroutine_dump.2019-01-01T00_20_00",
			},
		},
		{
			name: "total size smaller than size limit and unknown files are removed",
			files: []file{
				{name: "goroutine_dump.2019-01-01T00_00_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_10_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_20_00", size: 1},
				{name: "unknown_file", size: 1},
				{name: "unknown_file2", size: 1},
			},
			sizeLimit: 5,
			expected: []string{
				"goroutine_dump.2019-01-01T00_00_00",
				"goroutine_dump.2019-01-01T00_10_00",
				"goroutine_dump.2019-01-01T00_20_00",
			},
		},
		{
			name: "total size larger than size limit",
			files: []file{
				{name: "goroutine_dump.2019-01-01T00_00_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_10_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_20_00", size: 1},
				{name: "unknown_file", size: 1},
			},
			sizeLimit: 2,
			expected: []string{
				"goroutine_dump.2019-01-01T00_10_00",
				"goroutine_dump.2019-01-01T00_20_00",
			},
		},
		{
			name: "newest dump is already larger than size limit",
			files: []file{
				{name: "goroutine_dump.2019-01-01T00_00_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_10_00", size: 1},
				{name: "goroutine_dump.2019-01-01T00_20_00", size: 10},
				{name: "unknown_file", size: 1},
			},
			sizeLimit: 5,
			expected: []string{
				"goroutine_dump.2019-01-01T00_20_00",
			},
		},
		{
			name: "no dump in directory",
			files: []file{
				{name: "unknown_file", size: 1},
			},
			sizeLimit: 5,
			expected:  nil,
		},
	}

	dir := filepath.Join(os.TempDir(), "goroutine_dump")
	for _, c := range cases {
		func() {
			err := os.Mkdir(dir, 0755)
			assert.NoError(t, err, "unexpected error when making directory for testing")
			defer os.RemoveAll(dir)

			for _, f := range c.files {
				path := filepath.Join(dir, f.name)
				fi, err := os.Create(path)
				assert.NoError(t, err, "unexpected error when creating file %s", path)
				err = fi.Close()
				assert.NoError(t, err, "unexpected error when closing file %s", path)
				err = os.Truncate(path, f.size)
				assert.NoError(t, err, "unexpected error when truncating file %s", path)
			}
			ctx := context.TODO()
			gc(ctx, dir, c.sizeLimit)
			files, err := ioutil.ReadDir(dir)
			assert.NoError(t, err, "unexpected error when listing files in %s", dir)
			var actual []string
			for _, f := range files {
				actual = append(actual, f.Name())
			}
			assert.Equal(t, c.expected, actual)
		}()
	}
}

func TestTakeGoroutineDump(t *testing.T) {
	// takeGoroutineDump fails because "dump" already exists as a directory
	dir := os.TempDir()
	filename := "dump"
	path := filepath.Join(dir, filename)
	func() {
		err := os.Mkdir(path, 0755)
		assert.NoError(t, err, "failed to make dump directory")
		defer os.RemoveAll(path)

		err = takeGoroutineDump(dir, filename)
		assert.EqualError(
			t,
			err,
			"error creating file /tmp/dump for goroutine dump: open /tmp/dump: is a directory",
		)
	}()

	// takeGoroutineDump succeeds
	err := takeGoroutineDump(dir, filename)
	defer os.Remove(path)
	assert.NoError(t, err, "unexpected error when dumping goroutines")

	b, err := ioutil.ReadFile(path)
	assert.NoError(t, err, "unexpected error when reading from dump file")
	assert.Contains(t, string(b), "goroutine")
}
