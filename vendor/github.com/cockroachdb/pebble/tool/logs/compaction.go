// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package logs

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/spf13/cobra"
)

var (
	// Captures a common logging prefix that can be used as the context for the
	// surrounding information captured by other expressions. Example:
	//
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] ...
	//
	logContextPattern = regexp.MustCompile(
		`^.*` +
			/* Timestamp        */ `(?P<timestamp>\d{6} \d{2}:\d{2}:\d{2}.\d{6}).*` +
			/* Node / Store     */ `\[n(?P<node>\d+|\?),.*?,s(?P<store>\d+|\?).*?\].*`,
	)
	logContextPatternTimestampIdx = logContextPattern.SubexpIndex("timestamp")
	logContextPatternNodeIdx      = logContextPattern.SubexpIndex("node")
	logContextPatternStoreIdx     = logContextPattern.SubexpIndex("store")

	// Matches either a compaction or a memtable flush log line.
	//
	// A compaction start / end line resembles:
	//   "[JOB X] compact(ed|ing)"
	//
	// A memtable flush start / end line resembles:
	//   "[JOB X] flush(ed|ing)"
	sentinelPattern          = regexp.MustCompile(`\[JOB.*(?P<prefix>compact|flush|ingest)(?P<suffix>ed|ing)[^:]`)
	sentinelPatternPrefixIdx = sentinelPattern.SubexpIndex("prefix")
	sentinelPatternSuffixIdx = sentinelPattern.SubexpIndex("suffix")

	// Example compaction start and end log lines:
	//
	//   I211215 14:26:56.012382 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1845 ⋮ [n5,pebble,s5] 1216510  [JOB 284925] compacting(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M)
	//   I211215 14:26:56.318543 51831533 3@vendor/github.com/cockroachdb/pebble/compaction.go:1886 ⋮ [n5,pebble,s5] 1216554  [JOB 284925] compacted(default) L2 [442555] (4.2 M) + L3 [445853] (8.4 M) -> L3 [445883 445887] (13 M), in 0.3s, output rate 42 M/s
	//
	// NOTE: we use the log timestamp to compute the compaction duration rather
	// than the Pebble log output.
	compactionPattern = regexp.MustCompile(
		`^.*` +
			/* Job ID           */ `\[JOB (?P<job>\d+)]\s` +
			/* Start / end      */ `compact(?P<suffix>ed|ing)` +
			/* Compaction type  */ `\((?P<type>.*?)\)\s` +
			/* Start /end level */ `L(?P<from>\d)(?:.*(?:\+|->)\sL(?P<to>\d))?` +
			/* Bytes            */ `(?:.*?\((?P<digit>.*?)\s(?P<unit>.*?)\))?`,
	)
	compactionPatternJobIdx    = compactionPattern.SubexpIndex("job")
	compactionPatternSuffixIdx = compactionPattern.SubexpIndex("suffix")
	compactionPatternTypeIdx   = compactionPattern.SubexpIndex("type")
	compactionPatternFromIdx   = compactionPattern.SubexpIndex("from")
	compactionPatternToIdx     = compactionPattern.SubexpIndex("to")
	compactionPatternDigitIdx  = compactionPattern.SubexpIndex("digit")
	compactionPatternUnitIdx   = compactionPattern.SubexpIndex("unit")

	// Example memtable flush log lines:
	//
	//   I211213 16:23:48.903751 21136 3@vendor/github.com/cockroachdb/pebble/event.go:599 ⋮ [n9,pebble,s9] 24 [JOB 10] flushing 2 memtables to L0
	//   I211213 16:23:49.134464 21136 3@vendor/github.com/cockroachdb/pebble/event.go:603 ⋮ [n9,pebble,s9] 26 [JOB 10] flushed 2 memtables to L0 [1535806] (1.3 M), in 0.2s, output rate 5.8 M/s
	//
	// NOTE: we use the log timestamp to compute the flush duration rather than
	// the Pebble log output.
	flushPattern = regexp.MustCompile(
		`^..*` +
			/* Job ID          */ `\[JOB (?P<job>\d+)]\s` +
			/* Compaction type */ `flush(?P<suffix>ed|ing)` +
			/* Bytes           */ `(?:.*?\((?P<digit>.*?)\s(?P<unit>.*?)\))?`,
	)
	flushPatternSuffixIdx = flushPattern.SubexpIndex("suffix")
	flushPatternJobIdx    = flushPattern.SubexpIndex("job")
	flushPatternDigitIdx  = flushPattern.SubexpIndex("digit")
	flushPatternUnitIdx   = flushPattern.SubexpIndex("unit")

	// Example ingested log lines:
	//
	//   I220228 16:01:22.487906 18476248525 3@vendor/github.com/cockroachdb/pebble/ingest.go:637 ⋮ [n24,pebble,s24] 33430782  [JOB 10211226] ingested L0:21818678 (1.8 K), L0:21818683 (1.2 K), L0:21818679 (1.6 K), L0:21818680 (1.1 K), L0:21818681 (1.1 K), L0:21818682 (160 M)
	//
	ingestedPattern = regexp.MustCompile(
		`^.*` +
			/* Job ID           */ `\[JOB (?P<job>\d+)]\s` +
			/* ingested */ `ingested\s`)
	ingestedPatternJobIdx = ingestedPattern.SubexpIndex("job")
	ingestedFilePattern   = regexp.MustCompile(
		`L` +
			/* Level */ `(?P<level>\d):` +
			/* File number */ `(?P<file>\d+)\s` +
			/* Bytes */ `\((?P<value>.*?)\s(?P<unit>.*?)\)`)
	ingestedFilePatternLevelIdx = ingestedFilePattern.SubexpIndex("level")
	ingestedFilePatternFileIdx  = ingestedFilePattern.SubexpIndex("file")
	ingestedFilePatternValueIdx = ingestedFilePattern.SubexpIndex("value")
	ingestedFilePatternUnitIdx  = ingestedFilePattern.SubexpIndex("unit")

	// Example read-amp log line:
	//
	//   total     31766   188 G       -   257 G   187 G    48 K   3.6 G     744   536 G    49 K   278 G       5     2.1
	//
	readAmpPattern = regexp.MustCompile(
		/* Read-amp     */ `(?:^|\+)\s{2}total.*?(?P<value>\d+).{8}$`,
	)
	readAmpPatternValueIdx = readAmpPattern.SubexpIndex("value")
)

const (
	// timeFmt matches the Cockroach log timestamp format.
	// See: https://github.com/cockroachdb/cockroach/blob/master/pkg/util/log/format_crdb_v2.go
	timeFmt = "060102 15:04:05.000000"

	// timeFmtSlim is similar to timeFmt, except that it strips components with a
	// lower granularity than a minute.
	timeFmtSlim = "060102 15:04"

	// timeFmtHrMinSec prints only the hour, minute and second of the time.
	timeFmtHrMinSec = "15:04:05"
)

// compactionType is the type of compaction. It tracks the types in
// compaction.go. We copy the values here to avoid exporting the types in
// compaction.go.
type compactionType uint8

const (
	compactionTypeDefault compactionType = iota
	compactionTypeFlush
	compactionTypeMove
	compactionTypeDeleteOnly
	compactionTypeElisionOnly
	compactionTypeRead
)

// String implements fmt.Stringer.
func (c compactionType) String() string {
	switch c {
	case compactionTypeDefault:
		return "default"
	case compactionTypeMove:
		return "move"
	case compactionTypeDeleteOnly:
		return "delete-only"
	case compactionTypeElisionOnly:
		return "elision-only"
	case compactionTypeRead:
		return "read"
	default:
		panic(errors.Newf("unknown compaction type: %s", c))
	}
}

// parseCompactionType parses the given compaction type string and returns a
// compactionType.
func parseCompactionType(s string) (t compactionType, err error) {
	switch s {
	case "default":
		t = compactionTypeDefault
	case "move":
		t = compactionTypeMove
	case "delete-only":
		t = compactionTypeDeleteOnly
	case "elision-only":
		t = compactionTypeElisionOnly
	case "read":
		t = compactionTypeRead
	default:
		err = errors.Newf("unknown compaction type: %s", s)
	}
	return
}

// compactionStart is a compaction start event.
type compactionStart struct {
	ctx       logContext
	jobID     int
	cType     compactionType
	fromLevel int
	toLevel   int
}

// parseCompactionStart converts the given regular expression sub-matches for a
// compaction start log line into a compactionStart event.
func parseCompactionStart(matches []string) (compactionStart, error) {
	var start compactionStart

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[compactionPatternJobIdx])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}

	// Parse compaction type.
	cType, err := parseCompactionType(matches[compactionPatternTypeIdx])
	if err != nil {
		return start, err
	}

	// Parse from-level.
	from, err := strconv.Atoi(matches[compactionPatternFromIdx])
	if err != nil {
		return start, errors.Newf("could not parse from-level: %s", err)
	}

	// Parse to-level. For deletion and elision compactions, set the same level.
	to := from
	if cType != compactionTypeElisionOnly && cType != compactionTypeDeleteOnly {
		to, err = strconv.Atoi(matches[compactionPatternToIdx])
		if err != nil {
			return start, errors.Newf("could not parse to-level: %s", err)
		}
	}

	start = compactionStart{
		jobID:     jobID,
		cType:     cType,
		fromLevel: from,
		toLevel:   to,
	}

	return start, nil
}

// compactionEnd is a compaction end event.
type compactionEnd struct {
	jobID        int
	writtenBytes uint64
	// TODO(jackson): Parse and include the aggregate size of input
	// sstables. It may be instructive, because compactions that drop
	// keys write less data than they remove from the input level.
}

// parseCompactionEnd converts the given regular expression sub-matches for a
// compaction end log line into a compactionEnd event.
func parseCompactionEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[compactionPatternJobIdx])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}
	end = compactionEnd{jobID: jobID}

	// Optionally, if we have compacted bytes.
	if matches[compactionPatternDigitIdx] != "" {
		d, e := strconv.ParseFloat(matches[compactionPatternDigitIdx], 64)
		if e != nil {
			return end, errors.Newf("could not parse compacted bytes digit: %s", e)
		}
		end.writtenBytes = unHumanize(d, matches[compactionPatternUnitIdx])
	}

	return end, nil
}

// parseFlushStart converts the given regular expression sub-matches for a
// memtable flush start log line into a compactionStart event.
func parseFlushStart(matches []string) (compactionStart, error) {
	var start compactionStart
	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushPatternJobIdx])
	if err != nil {
		return start, errors.Newf("could not parse jobID: %s", err)
	}
	c := compactionStart{
		jobID:     jobID,
		cType:     compactionTypeFlush,
		fromLevel: -1,
		toLevel:   0,
	}
	return c, nil
}

// parseFlushEnd converts the given regular expression sub-matches for a
// memtable flush end log line into a compactionEnd event.
func parseFlushEnd(matches []string) (compactionEnd, error) {
	var end compactionEnd

	// Parse job ID.
	jobID, err := strconv.Atoi(matches[flushPatternJobIdx])
	if err != nil {
		return end, errors.Newf("could not parse jobID: %s", err)
	}
	end = compactionEnd{jobID: jobID}

	// Optionally, if we have flushed bytes.
	if matches[flushPatternDigitIdx] != "" {
		d, e := strconv.ParseFloat(matches[flushPatternDigitIdx], 64)
		if e != nil {
			return end, errors.Newf("could not parse flushed bytes digit: %s", e)
		}
		end.writtenBytes = unHumanize(d, matches[flushPatternUnitIdx])
	}

	return end, nil
}

// event describes an aggregated event (eg, start and end events
// combined if necessary).
type event struct {
	nodeID     int
	storeID    int
	jobID      int
	timeStart  time.Time
	timeEnd    time.Time
	compaction *compaction
	ingest     *ingest
}

// compaction represents an aggregated compaction event (i.e. the combination of
// a start and end event).
type compaction struct {
	cType        compactionType
	fromLevel    int
	toLevel      int
	writtenBytes uint64
}

// ingest describes the completion of an ingest.
type ingest struct {
	files []ingestedFile
}

type ingestedFile struct {
	level     int
	fileNum   int
	sizeBytes uint64
}

// readAmp represents a read-amp event.
type readAmp struct {
	ctx     logContext
	readAmp int
}

type nodeStoreJob struct {
	node, store, job int
}

func (n nodeStoreJob) String() string {
	return fmt.Sprintf("(node=%d,store=%d,job=%d)", n.node, n.store, n.job)
}

type errorEvent struct {
	path string
	line string
	err  error
}

// logEventCollector keeps track of open compaction events and read-amp events
// over the course of parsing log line events. Completed compaction events are
// added to the collector once a matching start and end pair are encountered.
// Read-amp events are added as they are encountered (the have no start / end
// concept).
type logEventCollector struct {
	ctx      logContext
	m        map[nodeStoreJob]compactionStart
	events   []event
	readAmps []readAmp
	errors   []errorEvent
}

// newEventCollector instantiates a new logEventCollector.
func newEventCollector() *logEventCollector {
	return &logEventCollector{
		m: make(map[nodeStoreJob]compactionStart),
	}
}

// addError records an error encountered during log parsing.
func (c *logEventCollector) addError(path, line string, err error) {
	c.errors = append(c.errors, errorEvent{path: path, line: line, err: err})
}

// addCompactionStart adds a new compactionStart to the collector. The event is
// tracked by its job ID.
func (c *logEventCollector) addCompactionStart(start compactionStart) error {
	key := nodeStoreJob{c.ctx.node, c.ctx.store, start.jobID}
	if _, ok := c.m[key]; ok {
		return errors.Newf("start event already seen for %s", key)
	}
	start.ctx = c.ctx
	c.m[key] = start
	return nil
}

// addCompactionEnd completes the compaction event for the given compactionEnd.
func (c *logEventCollector) addCompactionEnd(end compactionEnd) {
	key := nodeStoreJob{c.ctx.node, c.ctx.store, end.jobID}
	start, ok := c.m[key]
	if !ok {
		_, _ = fmt.Fprintf(
			os.Stderr,
			"compaction end event missing start event for %s; skipping\n", key,
		)
		return
	}

	// Remove the job from the collector once it has been matched.
	delete(c.m, key)

	c.events = append(c.events, event{
		nodeID:    start.ctx.node,
		storeID:   start.ctx.store,
		jobID:     start.jobID,
		timeStart: start.ctx.timestamp,
		timeEnd:   c.ctx.timestamp,
		compaction: &compaction{
			cType:        start.cType,
			fromLevel:    start.fromLevel,
			toLevel:      start.toLevel,
			writtenBytes: end.writtenBytes,
		},
	})
}

// addReadAmp adds the readAmp event to the collector.
func (c *logEventCollector) addReadAmp(ra readAmp) {
	ra.ctx = c.ctx
	c.readAmps = append(c.readAmps, ra)
}

// logContext captures the metadata of log lines.
type logContext struct {
	timestamp   time.Time
	node, store int
}

// saveContext saves the given logContext in the collector.
func (c *logEventCollector) saveContext(ctx logContext) {
	c.ctx = ctx
}

// level is a level in the LSM. The WAL is level -1.
type level int

// String implements fmt.Stringer.
func (l level) String() string {
	if l == -1 {
		return "WAL"
	}
	return "L" + strconv.Itoa(int(l))
}

// fromTo is a map key for (from, to) level tuples.
type fromTo struct {
	from, to level
}

// compactionTypeCount is a mapping from compaction type to count.
type compactionTypeCount map[compactionType]int

// windowSummary summarizes events in a window of time between a start and end
// time. The window tracks:
// - for each compaction type: counts, total bytes compacted, and total duration.
// - total ingested bytes for each level
// - read amp magnitudes
type windowSummary struct {
	nodeID, storeID  int
	tStart, tEnd     time.Time
	eventCount       int
	flushedCount     int
	flushedBytes     uint64
	flushedTime      time.Duration
	compactionCounts map[fromTo]compactionTypeCount
	compactionBytes  map[fromTo]uint64
	compactionTime   map[fromTo]time.Duration
	ingestedCount    [7]int
	ingestedBytes    [7]uint64
	readAmps         []readAmp
	longRunning      []event
}

// String implements fmt.Stringer, returning a formatted window summary.
func (s windowSummary) String() string {
	type fromToCount struct {
		ft       fromTo
		counts   compactionTypeCount
		bytes    uint64
		duration time.Duration
	}
	var pairs []fromToCount
	for k, v := range s.compactionCounts {
		pairs = append(pairs, fromToCount{
			ft:       k,
			counts:   v,
			bytes:    s.compactionBytes[k],
			duration: s.compactionTime[k],
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		l, r := pairs[i], pairs[j]
		if l.ft.from == r.ft.from {
			return l.ft.to < r.ft.to
		}
		return l.ft.from < r.ft.from
	})

	nodeID, storeID := "?", "?"
	if s.nodeID != -1 {
		nodeID = strconv.Itoa(s.nodeID)
	}
	if s.storeID != -1 {
		storeID = strconv.Itoa(s.storeID)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("node: %s, store: %s\n", nodeID, storeID))
	sb.WriteString(fmt.Sprintf("   from: %s\n", s.tStart.Format(timeFmtSlim)))
	sb.WriteString(fmt.Sprintf("     to: %s\n", s.tEnd.Format(timeFmtSlim)))
	var count, sum int
	for _, ra := range s.readAmps {
		count++
		sum += ra.readAmp
	}
	sb.WriteString(fmt.Sprintf("  r-amp: %.1f\n", float64(sum)/float64(count)))

	// Print flush+ingest statistics.
	{
		var headerWritten bool
		maybeWriteHeader := func() {
			if !headerWritten {
				sb.WriteString("_kind______from______to_____________________________________count___bytes______time\n")
				headerWritten = true
			}
		}

		if s.flushedCount > 0 {
			maybeWriteHeader()
			fmt.Fprintf(&sb, "%-7s         %7s                                   %7d %7s %9s\n",
				"flush", "L0", s.flushedCount, humanize.Uint64(s.flushedBytes),
				s.flushedTime.Truncate(time.Second))
		}

		count := s.flushedCount
		sum := s.flushedBytes
		totalTime := s.flushedTime
		for l := 0; l < len(s.ingestedBytes); l++ {
			if s.ingestedCount[l] == 0 {
				continue
			}
			maybeWriteHeader()
			fmt.Fprintf(&sb, "%-7s         %7s                                   %7d %7s\n",
				"ingest", fmt.Sprintf("L%d", l), s.ingestedCount[l], humanize.Uint64(s.ingestedBytes[l]))
			count += s.ingestedCount[l]
			sum += s.ingestedBytes[l]
		}
		if headerWritten {
			fmt.Fprintf(&sb, "total                                                     %7d %7s %9s\n",
				count, humanize.Uint64(sum), totalTime.Truncate(time.Second),
			)
		}
	}

	// Print compactions statistics.
	if len(s.compactionCounts) > 0 {
		sb.WriteString("_kind______from______to___default____move___elide__delete___count___bytes______time\n")
		var totalDef, totalMove, totalElision, totalDel int
		var totalBytes uint64
		var totalTime time.Duration
		for _, p := range pairs {
			def := p.counts[compactionTypeDefault]
			move := p.counts[compactionTypeMove]
			elision := p.counts[compactionTypeElisionOnly]
			del := p.counts[compactionTypeDeleteOnly]
			total := def + move + elision + del

			str := fmt.Sprintf("%-7s %7s %7s   %7d %7d %7d %7d %7d %7s %9s\n",
				"compact", p.ft.from, p.ft.to, def, move, elision, del, total,
				humanize.Uint64(p.bytes), p.duration.Truncate(time.Second))
			sb.WriteString(str)

			totalDef += def
			totalMove += move
			totalElision += elision
			totalDel += del
			totalBytes += p.bytes
			totalTime += p.duration
		}
		sb.WriteString(fmt.Sprintf("total         %19d %7d %7d %7d %7d %7s %9s\n",
			totalDef, totalMove, totalElision, totalDel, s.eventCount,
			humanize.Uint64(totalBytes), totalTime.Truncate(time.Second)))
	}

	// (Optional) Long running events.
	if len(s.longRunning) > 0 {
		sb.WriteString("long-running events (descending runtime):\n")
		sb.WriteString("_kind________from________to_______job______type_____start_______end____dur(s)_____bytes:\n")
		for _, e := range s.longRunning {
			c := e.compaction
			kind := "compact"
			if c.fromLevel == -1 {
				kind = "flush"
			}
			sb.WriteString(fmt.Sprintf("%-7s %9s %9s %9d %9s %9s %9s %9.0f %9s\n",
				kind, level(c.fromLevel), level(c.toLevel), e.jobID, c.cType,
				e.timeStart.Format(timeFmtHrMinSec), e.timeEnd.Format(timeFmtHrMinSec),
				e.timeEnd.Sub(e.timeStart).Seconds(), humanize.Uint64(c.writtenBytes)))
		}
	}

	return sb.String()
}

// windowSummarySlice is a slice of windowSummary that sorts in order of start
// time, node, then store.
type windowsSummarySlice []windowSummary

func (s windowsSummarySlice) Len() int {
	return len(s)
}

func (s windowsSummarySlice) Less(i, j int) bool {
	if !s[i].tStart.Equal(s[j].tStart) {
		return s[i].tStart.Before(s[j].tStart)
	}
	if s[i].nodeID != s[j].nodeID {
		return s[i].nodeID < s[j].nodeID
	}
	return s[i].storeID < s[j].storeID
}

func (s windowsSummarySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// eventSlice is a slice of events that sorts in order of node, store,
// then event start time.
type eventSlice []event

func (s eventSlice) Len() int {
	return len(s)
}

func (s eventSlice) Less(i, j int) bool {
	if s[i].nodeID != s[j].nodeID {
		return s[i].nodeID < s[j].nodeID
	}
	if s[i].storeID != s[j].storeID {
		return s[i].storeID < s[j].storeID
	}
	return s[i].timeStart.Before(s[j].timeStart)
}

func (s eventSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// readAmpSlice is a slice of readAmp events that sorts in order of node, store,
// then read amp event start time.
type readAmpSlice []readAmp

func (r readAmpSlice) Len() int {
	return len(r)
}

func (r readAmpSlice) Less(i, j int) bool {
	// Sort by node, store, then read-amp.
	if r[i].ctx.node != r[j].ctx.node {
		return r[i].ctx.node < r[j].ctx.node
	}
	if r[i].ctx.store != r[j].ctx.store {
		return r[i].ctx.store < r[j].ctx.store
	}
	return r[i].ctx.timestamp.Before(r[j].ctx.timestamp)
}

func (r readAmpSlice) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// aggregator combines compaction and read-amp events within windows of fixed
// duration and returns one aggregated windowSummary struct per window.
type aggregator struct {
	window           time.Duration
	events           []event
	readAmps         []readAmp
	longRunningLimit time.Duration
}

// newAggregator returns a new aggregator.
func newAggregator(
	window, longRunningLimit time.Duration, events []event, readAmps []readAmp,
) *aggregator {
	return &aggregator{
		window:           window,
		events:           events,
		readAmps:         readAmps,
		longRunningLimit: longRunningLimit,
	}
}

// aggregate aggregates the events into windows, returning the windowSummary for
// each interval.
func (a *aggregator) aggregate() []windowSummary {
	if len(a.events) == 0 {
		return nil
	}

	// Sort the event and read-amp slices by start time.
	sort.Sort(eventSlice(a.events))
	sort.Sort(readAmpSlice(a.readAmps))

	initWindow := func(e event) *windowSummary {
		start := e.timeStart.Truncate(a.window)
		return &windowSummary{
			nodeID:           e.nodeID,
			storeID:          e.storeID,
			tStart:           start,
			tEnd:             start.Add(a.window),
			compactionCounts: make(map[fromTo]compactionTypeCount),
			compactionBytes:  make(map[fromTo]uint64),
			compactionTime:   make(map[fromTo]time.Duration),
		}
	}

	var windows []windowSummary
	var j int // index for read-amps
	finishWindow := func(cur *windowSummary) {
		// Collect read-amp values for the previous window.
		var readAmps []readAmp
		for j < len(a.readAmps) {
			ra := a.readAmps[j]

			// Skip values before the current window.
			if ra.ctx.node < cur.nodeID ||
				ra.ctx.store < cur.storeID ||
				ra.ctx.timestamp.Before(cur.tStart) {
				j++
				continue
			}

			// We've passed over the current window. Stop.
			if ra.ctx.node > cur.nodeID ||
				ra.ctx.store > cur.storeID ||
				ra.ctx.timestamp.After(cur.tEnd) {
				break
			}

			// Collect this read-amp value.
			readAmps = append(readAmps, ra)
			j++
		}
		cur.readAmps = readAmps

		// Sort long running compactions in descending order of duration.
		sort.Slice(cur.longRunning, func(i, j int) bool {
			l := cur.longRunning[i]
			r := cur.longRunning[j]
			return l.timeEnd.Sub(l.timeStart) > r.timeEnd.Sub(r.timeStart)
		})

		// Add the completed window to the set of windows.
		windows = append(windows, *cur)
	}

	// Move through the compactions, collecting relevant compactions into the same
	// window. Windows have the same node and store, and a compaction start time
	// within a given range.
	i := 0
	curWindow := initWindow(a.events[i])
	for ; ; i++ {
		// No more windows. Complete the current window.
		if i == len(a.events) {
			finishWindow(curWindow)
			break
		}
		e := a.events[i]

		// If we're at the start of a new interval, finalize the current window and
		// start a new one.
		if curWindow.nodeID != e.nodeID ||
			curWindow.storeID != e.storeID ||
			e.timeStart.After(curWindow.tEnd) {
			finishWindow(curWindow)
			curWindow = initWindow(e)
		}

		switch {
		case e.ingest != nil:
			// Update ingest stats.
			for _, f := range e.ingest.files {
				curWindow.ingestedCount[f.level]++
				curWindow.ingestedBytes[f.level] += f.sizeBytes
			}
		case e.compaction != nil && e.compaction.cType == compactionTypeFlush:
			// Update flush stats.
			f := e.compaction
			curWindow.flushedCount++
			curWindow.flushedBytes += f.writtenBytes
			curWindow.flushedTime += e.timeEnd.Sub(e.timeStart)
		case e.compaction != nil:
			// Update compaction stats.
			c := e.compaction
			// Update compaction counts.
			ft := fromTo{level(c.fromLevel), level(c.toLevel)}
			m, ok := curWindow.compactionCounts[ft]
			if !ok {
				m = make(compactionTypeCount)
				curWindow.compactionCounts[ft] = m
			}
			m[c.cType]++
			curWindow.eventCount++

			// Update compacted bytes.
			_, ok = curWindow.compactionBytes[ft]
			if !ok {
				curWindow.compactionBytes[ft] = 0
			}
			curWindow.compactionBytes[ft] += c.writtenBytes

			// Update compaction time.
			_, ok = curWindow.compactionTime[ft]
			if !ok {
				curWindow.compactionTime[ft] = 0
			}
			curWindow.compactionTime[ft] += e.timeEnd.Sub(e.timeStart)

		}
		// Add "long-running" events. Those that start in this window
		// that have duration longer than the window interval.
		if e.timeEnd.Sub(e.timeStart) > a.longRunningLimit {
			curWindow.longRunning = append(curWindow.longRunning, e)
		}
	}

	// Windows are added in order of (node, store, time). Re-sort the windows by
	// (time, node, store) for better presentation.
	sort.Sort(windowsSummarySlice(windows))

	return windows
}

// parseLog parses the log file with the given path, using the given parse
// function to collect events in the given logEventCollector. parseLog
// returns a non-nil error if an I/O error was encountered while reading
// the log file. Parsing errors are accumulated in the
// logEventCollector.
func parseLog(path string, b *logEventCollector) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()

		// Store the log context for the current line, if we have one.
		if err := parseLogContext(line, b); err != nil {
			return err
		}

		// First check for a flush or compaction.
		matches := sentinelPattern.FindStringSubmatch(line)
		if matches != nil {
			// Determine which regexp to apply by testing the first letter of the prefix.
			var err error
			switch matches[sentinelPatternPrefixIdx][0] {
			case 'c':
				err = parseCompaction(line, b)
			case 'f':
				err = parseFlush(line, b)
			case 'i':
				err = parseIngest(line, b)
			default:
				err = errors.Newf("unexpected line: neither compaction nor flush: %s", line)
			}
			if err != nil {
				b.addError(path, line, err)
			}
			continue
		}

		// Else check for an LSM debug line.
		if err = parseReadAmp(line, b); err != nil {
			b.addError(path, line, err)
			continue
		}
	}
	return s.Err()
}

// parseLogContext extracts contextual information from the log line (e.g. the
// timestamp, node and store).
func parseLogContext(line string, b *logEventCollector) error {
	matches := logContextPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	// Parse start time.
	t, err := time.Parse(timeFmt, matches[logContextPatternTimestampIdx])
	if err != nil {
		return errors.Newf("could not parse timestamp: %s", err)
	}

	// Parse node and store.
	nodeID, err := strconv.Atoi(matches[logContextPatternNodeIdx])
	if err != nil {
		if matches[logContextPatternNodeIdx] != "?" {
			return errors.Newf("could not parse node ID: %s", err)
		}
		nodeID = -1
	}

	storeID, err := strconv.Atoi(matches[logContextPatternStoreIdx])
	if err != nil {
		if matches[logContextPatternStoreIdx] != "?" {
			return errors.Newf("could not parse store ID: %s", err)
		}
		storeID = -1
	}

	b.saveContext(logContext{
		timestamp: t,
		node:      nodeID,
		store:     storeID,
	})
	return nil
}

// parseCompaction parses and collects Pebble compaction events.
func parseCompaction(line string, b *logEventCollector) error {
	matches := compactionPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 8 {
		return errors.Newf(
			"could not parse compaction start / end line; found %d matches: %s",
			len(matches), line)
	}

	// "compacting": implies start line.
	if matches[compactionPatternSuffixIdx] == "ing" {
		start, err := parseCompactionStart(matches)
		if err != nil {
			return err
		}
		if err := b.addCompactionStart(start); err != nil {
			return err
		}
		return nil
	}

	// "compacted": implies end line.
	end, err := parseCompactionEnd(matches)
	if err != nil {
		return err
	}

	b.addCompactionEnd(end)
	return nil
}

// parseFlush parses and collects Pebble memtable flush events.
func parseFlush(line string, b *logEventCollector) error {
	matches := flushPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}

	if len(matches) != 5 {
		return errors.Newf(
			"could not parse flush start / end line; found %d matches: %s",
			len(matches), line)
	}

	if matches[flushPatternSuffixIdx] == "ing" {
		start, err := parseFlushStart(matches)
		if err != nil {
			return err
		}
		return b.addCompactionStart(start)
	}

	end, err := parseFlushEnd(matches)
	if err != nil {
		return err
	}

	b.addCompactionEnd(end)
	return nil
}

// parseIngest parses and collects Pebble ingest complete events.
func parseIngest(line string, b *logEventCollector) error {
	matches := ingestedPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}
	// Parse job ID.
	jobID, err := strconv.Atoi(matches[ingestedPatternJobIdx])
	if err != nil {
		return errors.Newf("could not parse jobID: %s", err)
	}
	fileMatches := ingestedFilePattern.FindAllStringSubmatch(line, -1)
	files := make([]ingestedFile, len(fileMatches))
	for i := range fileMatches {
		level, err := strconv.Atoi(fileMatches[i][ingestedFilePatternLevelIdx])
		if err != nil {
			return errors.Newf("could not parse level: %s", err)
		}
		fileNum, err := strconv.Atoi(fileMatches[i][ingestedFilePatternFileIdx])
		if err != nil {
			return errors.Newf("could not parse file number: %s", err)
		}
		v, err := strconv.ParseFloat(fileMatches[i][ingestedFilePatternValueIdx], 64)
		if err != nil {
			return errors.Newf("could not parse file size value: %s", err)
		}
		files = append(files, ingestedFile{
			level:     level,
			fileNum:   fileNum,
			sizeBytes: unHumanize(v, fileMatches[i][ingestedFilePatternUnitIdx]),
		})
	}
	b.events = append(b.events, event{
		nodeID:    b.ctx.node,
		storeID:   b.ctx.store,
		jobID:     jobID,
		timeStart: b.ctx.timestamp,
		timeEnd:   b.ctx.timestamp,
		ingest: &ingest{
			files: files,
		},
	})

	return nil
}

// parseReadAmp attempts to parse the current line as a read amp value
func parseReadAmp(line string, b *logEventCollector) error {
	matches := readAmpPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil
	}
	val, err := strconv.Atoi(matches[readAmpPatternValueIdx])
	if err != nil {
		return errors.Newf("could not parse read amp: %s", err)
	}
	b.addReadAmp(readAmp{
		readAmp: val,
	})
	return nil
}

// runCompactionLogs is runnable function of the top-level cobra.Command that
// parses and collects Pebble compaction events and LSM information.
func runCompactionLogs(cmd *cobra.Command, args []string) error {
	// The args contain a list of log files to read.
	files := args

	// Scan the log files collecting start and end compaction lines.
	b := newEventCollector()
	for _, file := range files {
		err := parseLog(file, b)
		// parseLog returns an error only on I/O errors, which we
		// immediately exit with.
		if err != nil {
			return err
		}
	}

	window, err := cmd.Flags().GetDuration("window")
	if err != nil {
		return err
	}

	longRunningLimit, err := cmd.Flags().GetDuration("long-running-limit")
	if err != nil {
		return err
	}
	if longRunningLimit == 0 {
		// Off by default. Set to infinite duration.
		longRunningLimit = time.Duration(math.MaxInt64)
	}

	// Aggregate the lines.
	a := newAggregator(window, longRunningLimit, b.events, b.readAmps)
	summaries := a.aggregate()
	for _, s := range summaries {
		fmt.Printf("%s\n", s)
	}

	// After the summaries, print accumulated parsing errors to stderr.
	for _, e := range b.errors {
		fmt.Fprintf(os.Stderr, "-\n%s: %s\nError: %s\n", filepath.Base(e.path), e.line, e.err)
	}
	return nil
}

// unHumanize performs the opposite of humanize.Uint64, converting a
// human-readable digit and unit into a raw number of bytes.
func unHumanize(d float64, u string) uint64 {
	if u == "" {
		return uint64(d)
	}

	multiplier := uint64(1)
	switch u {
	case "B":
		// no-op: treat as regular bytes.
	case "K":
		multiplier = 1 << 10
	case "M":
		multiplier = 1 << 20
	case "G":
		multiplier = 1 << 30
	case "T":
		multiplier = 1 << 40
	case "P":
		multiplier = 1 << 50
	case "E":
		multiplier = 1 << 60
	default:
		panic(errors.Newf("unknown unit %q", u))
	}

	return uint64(d) * multiplier
}
