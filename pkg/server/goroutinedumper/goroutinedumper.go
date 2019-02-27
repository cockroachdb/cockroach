package goroutinedumper

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	goroutineDumpPrefix = "goroutine_dump"
	minDumpInterval     = time.Minute
	timeFormat          = "2006-01-02T15_04_05.999"
)

var (
	numGoroutinesThreshold = settings.RegisterIntSetting(
		"server.goroutine_dump.num_goroutines_threshold",
		"a threshold beyond which if number of goroutines increases, "+
			"then goroutine dump is triggered",
		10000,
	)
	growthRateThreshold = settings.RegisterFloatSetting(
		"server.goroutine_dump.growth_rate_threshold",
		"a threshold t where if number of goroutines increases to t times "+
			"in one sampling interval, then goroutine dump is triggered",
		2.0,
	)
	lowerLimitForNumGoroutines = settings.RegisterIntSetting(
		"server.goroutine_dump.lower_limit_for_num_goroutines",
		"number of goroutines below which goroutine dump will never be triggered",
		2000,
	)
	totalDumpSizeLimit = settings.RegisterByteSizeSetting(
		"server.goroutine_dump.total_dump_size_limit",
		"total size of goroutine dumps to be kept. "+
			"Dumps are GC'ed in the order of creation time. The latest dump is "+
			"always kept even if its size exceeds the limit",
		500<<20, // 500MiB
	)
)

// heuristic represents whether goroutine dump is triggered. It is true when
// we think a goroutine dump is helpful in debugging OOM issues.
type heuristic struct {
	name   string
	isTrue func(s *GoroutineDumper, st *cluster.Settings) bool
}

var numExceedThresholdHeuristic = heuristic{
	name: "num_exceed_threshold",
	isTrue: func(s *GoroutineDumper, st *cluster.Settings) bool {
		return s.goroutines > numGoroutinesThreshold.Get(&st.SV)
	},
}

var growTooFastSinceLastCheckHeuristic = heuristic{
	name: "grow_too_fast_since_last_check",
	isTrue: func(s *GoroutineDumper, st *cluster.Settings) bool {
		return s.goroutines > lowerLimitForNumGoroutines.Get(&st.SV) &&
			float64(s.goroutines) > growthRateThreshold.Get(&st.SV)*float64(s.lastCheckGoroutines)
	},
}

// GoroutineDumper stores relevant functions and stats to take goroutine dumps
// if an abnormal change in number of goroutines is detected.
type GoroutineDumper struct {
	goroutines          int64
	lastCheckGoroutines int64
	heuristics          []heuristic
	currentTime         func() time.Time
	lastDumpTime        time.Time
	takeGoroutineDump   func(dir string, filename string) error
	gc                  func(ctx context.Context, dir string, sizeLimit int64)
	dir                 string
}

// MaybeDump takes a goroutine dump only when both conditions are met:
//   1. at least one heuristic in GoroutineDumper is true
//   2. no dump was taken in last minDumpInterval
// At most one dump is taken in a call of this function.
func (gd *GoroutineDumper) MaybeDump(ctx context.Context, st *cluster.Settings, goroutines int64) {
	gd.goroutines = goroutines
	for _, h := range gd.heuristics {
		if h.isTrue(gd, st) && gd.currentTime().Sub(gd.lastDumpTime) >= minDumpInterval {
			filename := fmt.Sprintf(
				"%s.%s.%s.%09d",
				goroutineDumpPrefix,
				gd.currentTime().Format(timeFormat),
				h.name,
				goroutines,
			)
			if err := gd.takeGoroutineDump(gd.dir, filename); err != nil {
				log.Errorf(ctx, "error dumping goroutines: %s", err)
				continue
			}
			gd.lastDumpTime = gd.currentTime()
			gd.gc(ctx, gd.dir, totalDumpSizeLimit.Get(&st.SV))
			break
		}
	}
	gd.lastCheckGoroutines = goroutines
}

// NewGoroutineDumper returns a GoroutineDumper which enables both heuristics.
// dir is the directory in which dumps are stored.
func NewGoroutineDumper(dir string) (*GoroutineDumper, error) {
	if dir == "" {
		return nil, errors.New("directory to store dumps could not be determined")
	}
	dir = filepath.Join(dir, "goroutine_dump")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	gd := &GoroutineDumper{
		heuristics: []heuristic{
			numExceedThresholdHeuristic,
			growTooFastSinceLastCheckHeuristic,
		},
		currentTime:       timeutil.Now,
		lastDumpTime:      timeutil.UnixEpoch,
		takeGoroutineDump: takeGoroutineDump,
		gc:                gc,
		dir:               dir,
	}
	return gd, nil
}

// gc removes oldest dumps when the total size of all dumps is larger
// than sizeLimit. Requires that the name of the dumps indicates dump time
// such that sorting the filenames corresponds to ordering the dumps
// from oldest to newest.
// Newest dump in the directory is not considered for GC.
func gc(ctx context.Context, dir string, sizeLimit int64) {
	// ReadDir returns a list of directory entries sorted by filename, which means
	// it is sorted by dump time.
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Errorf(ctx, "cannot read directory %s, err: %s", dir, err)
		return
	}

	var totalSize int64
	isLatestDump := true
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		path := filepath.Join(dir, f.Name())
		if strings.HasPrefix(f.Name(), goroutineDumpPrefix) {
			totalSize += f.Size()
			// Skipping the latest dump in gc
			if isLatestDump {
				isLatestDump = false
				continue
			}
			if totalSize > sizeLimit {
				if err := os.Remove(path); err != nil {
					log.Warningf(ctx, "Cannot remove dump file %s, err: %s", path, err)
				}
			}
		} else {
			log.Infof(ctx, "Removing unknown file %s in goroutine dump dir %s", f.Name(), dir)
			if err := os.Remove(path); err != nil {
				log.Warningf(ctx, "Cannot remove file %s, err: %s", path, err)
			}
		}
	}
}

func takeGoroutineDump(dir string, filename string) error {
	path := filepath.Join(dir, filename)
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "error creating file %s for goroutine dump", path)
	}
	defer f.Close()
	if err = pprof.Lookup("goroutine").WriteTo(f, 2); err != nil {
		return errors.Wrapf(err, "error writing goroutine dump to %s", path)
	}
	return nil
}
