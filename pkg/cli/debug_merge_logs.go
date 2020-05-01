// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/sync/errgroup"
)

type logStream interface {
	fileInfo() *fileInfo // FileInfo for the current entry available in peek.
	peek() (log.Entry, bool)
	pop() (log.Entry, bool) // If called after peek, must return the same values.
	error() error
}

// writeLogStream pops messages off of s and writes them to out prepending
// prefix per message and filtering messages which match filter.
func writeLogStream(s logStream, out io.Writer, filter *regexp.Regexp, prefix string) error {
	const chanSize = 1 << 16        // 64k
	const maxWriteBufSize = 1 << 18 // 256kB

	prefixCache := map[*fileInfo][]byte{}
	getPrefix := func(fi *fileInfo) ([]byte, error) {
		if prefixBuf, ok := prefixCache[fi]; ok {
			return prefixBuf, nil
		}
		prefixCache[fi] = fi.pattern.ExpandString(nil, prefix, fi.path, fi.matches)
		return prefixCache[fi], nil
	}

	type entryInfo struct {
		log.Entry
		*fileInfo
	}
	render := func(ei entryInfo, w io.Writer) (err error) {
		var prefixBytes []byte
		if prefixBytes, err = getPrefix(ei.fileInfo); err != nil {
			return err
		}
		if _, err = w.Write(prefixBytes); err != nil {
			return err
		}
		return ei.Format(w)
	}

	g, ctx := errgroup.WithContext(context.Background())
	entryChan := make(chan entryInfo, chanSize) // read -> bufferWrites
	writeChan := make(chan *bytes.Buffer)       // bufferWrites -> write
	read := func() error {
		defer close(entryChan)
		for e, ok := s.peek(); ok; e, ok = s.peek() {
			select {
			case entryChan <- entryInfo{Entry: e, fileInfo: s.fileInfo()}:
			case <-ctx.Done():
				return nil
			}
			s.pop()
		}
		return s.error()
	}
	bufferWrites := func() error {
		defer close(writeChan)
		writing, pending := &bytes.Buffer{}, &bytes.Buffer{}
		for {
			send, recv := writeChan, entryChan
			var scratch []byte
			if pending.Len() == 0 {
				send = nil
				if recv == nil {
					return nil
				}
			} else if pending.Len() > maxWriteBufSize {
				recv = nil
			}
			select {
			case ei, open := <-recv:
				if !open {
					entryChan = nil
					break
				}
				startLen := pending.Len()
				if err := render(ei, pending); err != nil {
					return err
				}
				if filter != nil {
					matches := filter.FindSubmatch(pending.Bytes()[startLen:])
					if matches == nil {
						// Did not match.
						pending.Truncate(startLen)
					} else if len(matches) > 1 {
						// Matched and there are capturing groups (if there
						// aren't any we'll want to print the whole message). We
						// only want to print what was captured. This is mildly
						// awkward since we can't output anything directly, so
						// we write the submatches back into the buffer and then
						// discard what we've looked at so far.
						//
						// NB: it's tempting to try to write into `pending`
						// directly, and while it's probably safe to do so
						// (despite truncating before writing back from the
						// truncated buffer into itself), it's not worth the
						// complexity in this code. Just use a scratch buffer.
						scratch = scratch[:0]
						for _, b := range matches[1:] {
							scratch = append(scratch, b...)
						}
						pending.Truncate(startLen)
						_, _ = pending.Write(scratch)
						pending.WriteByte('\n')
					}
				}
			case send <- pending:
				writing.Reset()
				pending, writing = writing, pending
			case <-ctx.Done():
				return nil
			}
		}
	}
	write := func() error {
		for buf := range writeChan {
			if _, err := out.Write(buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	}
	g.Go(read)
	g.Go(bufferWrites)
	g.Go(write)
	return g.Wait()
}

// mergedStream is a merged heap of log streams.
type mergedStream []logStream

// newMergedStreamFromPatterns creates a new logStream by first glob
// expanding pattern, then filtering for matching files which conform to the
// filePattern and if program is non-nil, they match the program which is
// extracted from matching files via the named capture group with the name
// "program". The returned stream will only return log entries in [from, to].
// If no program capture group exists all files match.
func newMergedStreamFromPatterns(
	ctx context.Context,
	patterns []string,
	filePattern, programFilter *regexp.Regexp,
	from, to time.Time,
) (logStream, error) {
	paths, err := expandPatterns(patterns)
	if err != nil {
		return nil, err
	}
	files, err := findLogFiles(paths, filePattern, programFilter,
		groupIndex(filePattern, "program"), to)
	if err != nil {
		return nil, err
	}
	return newMergedStream(ctx, files, from, to)
}

func groupIndex(re *regexp.Regexp, groupName string) int {
	for i, n := range re.SubexpNames() {
		if n == groupName {
			return i
		}
	}
	return -1
}

func newMergedStream(
	ctx context.Context, files []fileInfo, from, to time.Time,
) (*mergedStream, error) {
	// TODO(ajwerner): think about clock movement and PID
	const maxConcurrentFiles = 256 // should be far less than the FD limit
	sem := make(chan struct{}, maxConcurrentFiles)
	res := make(mergedStream, len(files))
	g, _ := errgroup.WithContext(ctx)
	createFileStream := func(i int) func() error {
		return func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			s, err := newFileLogStream(files[i], from, to)
			if s != nil {
				res[i] = s
			}
			return err
		}
	}
	for i := range files {
		g.Go(createFileStream(i))
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	g, ctx = errgroup.WithContext(ctx)
	filtered := res[:0] // filter nil streams
	for _, s := range res {
		if s != nil {
			s = newBufferedLogStream(ctx, g, s)
			filtered = append(filtered, s)
		}
	}
	res = filtered
	heap.Init(&res)
	return &res, nil
}

func (l mergedStream) Len() int      { return len(l) }
func (l mergedStream) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l mergedStream) Less(i, j int) bool {
	ie, iok := l[i].peek()
	je, jok := l[j].peek()
	if iok && jok {
		return ie.Time < je.Time
	}
	return !iok && jok
}

func (l *mergedStream) Push(s interface{}) {
	*l = append(*l, s.(logStream))
}

func (l *mergedStream) Pop() (v interface{}) {
	n := len(*l) - 1
	v = (*l)[n]
	*l = (*l)[:n]
	return
}

func (l *mergedStream) peek() (log.Entry, bool) {
	if len(*l) == 0 {
		return log.Entry{}, false
	}
	return (*l)[0].peek()
}

func (l *mergedStream) pop() (log.Entry, bool) {
	e, ok := l.peek()
	if !ok {
		return log.Entry{}, false
	}
	s := (*l)[0]
	s.pop()
	if _, stillOk := s.peek(); stillOk {
		heap.Push(l, heap.Pop(l))
	} else if err := s.error(); err != nil && err != io.EOF {
		return log.Entry{}, false
	} else {
		heap.Pop(l)
	}
	return e, true
}

func (l *mergedStream) fileInfo() *fileInfo {
	if len(*l) == 0 {
		return nil
	}
	return (*l)[0].fileInfo()
}

func (l *mergedStream) error() error {
	if len(*l) == 0 {
		return nil
	}
	return (*l)[0].error()
}

func expandPatterns(patterns []string) ([]string, error) {
	var paths []string
	for _, p := range patterns {
		matches, err := filepath.Glob(p)
		if err != nil {
			return nil, err
		}
		paths = append(paths, matches...)
	}
	return removeDuplicates(paths), nil
}

func removeDuplicates(strings []string) (filtered []string) {
	filtered = strings[:0]
	prev := ""
	for _, s := range strings {
		if s == prev {
			continue
		}
		filtered = append(filtered, s)
		prev = s
	}
	return filtered
}

func getLogFileInfo(path string, filePattern *regexp.Regexp) (fileInfo, bool) {
	if matches := filePattern.FindStringSubmatchIndex(path); matches != nil {
		return fileInfo{path: path, matches: matches, pattern: filePattern}, true
	}
	return fileInfo{}, false
}

type fileInfo struct {
	path    string
	pattern *regexp.Regexp
	matches []int
}

func findLogFiles(
	paths []string, filePattern, programFilter *regexp.Regexp, programGroup int, to time.Time,
) ([]fileInfo, error) {
	to = to.Truncate(time.Second) // log files only have second resolution
	fileChan := make(chan fileInfo, len(paths))
	var wg sync.WaitGroup
	wg.Add(len(paths))
	for _, p := range paths {
		go func(p string) {
			defer wg.Done()
			fi, ok := getLogFileInfo(p, filePattern)
			if !ok {
				return
			}
			if programGroup > 0 {
				program := fi.path[fi.matches[2*programGroup]:fi.matches[2*programGroup+1]]
				if !programFilter.MatchString(program) {
					return
				}
			}
			fileChan <- fi
		}(p)
	}
	wg.Wait()
	files := make([]fileInfo, 0, len(fileChan))
	close(fileChan)
	for f := range fileChan {
		files = append(files, f)
	}
	return files, nil
}

// newBufferedLogStream creates a new logStream which will create a goroutine to
// pop off of s and buffer in memory up to readChanSize entries.
// The channel and goroutine are created lazily upon the first read after the
// first call to pop. The initial peek does not consume resources.
func newBufferedLogStream(ctx context.Context, g *errgroup.Group, s logStream) logStream {
	bs := &bufferedLogStream{ctx: ctx, logStream: s, g: g}
	// Fill the initial entry value to prevent initial peek from running the
	// goroutine.
	bs.e, bs.ok = s.peek()
	bs.read = true
	s.pop()
	return bs
}

type bufferedLogStream struct {
	logStream
	runOnce sync.Once
	e       log.Entry
	read    bool
	ok      bool
	c       chan log.Entry
	ctx     context.Context
	g       *errgroup.Group
}

func (bs *bufferedLogStream) run() {
	const readChanSize = 512
	bs.c = make(chan log.Entry, readChanSize)
	bs.g.Go(func() error {
		defer close(bs.c)
		for {
			e, ok := bs.logStream.pop()
			if !ok {
				if err := bs.error(); err != io.EOF {
					return err
				}
				return nil
			}
			select {
			case bs.c <- e:
			case <-bs.ctx.Done():
				return nil
			}
		}
	})
}

func (bs *bufferedLogStream) peek() (log.Entry, bool) {
	if bs.ok && !bs.read {
		if bs.c == nil { // indicates that run has not been called
			bs.runOnce.Do(bs.run)
		}
		bs.e, bs.ok = <-bs.c
		bs.read = true
	}
	if !bs.ok {
		return log.Entry{}, false
	}
	return bs.e, true
}

func (bs *bufferedLogStream) pop() (log.Entry, bool) {
	e, ok := bs.peek()
	bs.read = false
	return e, ok
}

// fileLogStream represents a logStream from a single file.
type fileLogStream struct {
	from, to time.Time
	prevTime int64
	fi       fileInfo
	f        *os.File
	d        *log.EntryDecoder
	read     bool

	e   log.Entry
	err error
}

// newFileLogStream constructs a *fileLogStream and then peeks at the file to
// ensure that it contains a valid entry after from. If the file contains only
// data which precedes from, (nil, nil) is returned. If an io error is
// encountered during the initial peek, that error is returned. The underlying
// file is always closed before returning from this constructor so the initial
// peek does not consume resources.
func newFileLogStream(fi fileInfo, from, to time.Time) (logStream, error) {
	s := &fileLogStream{
		fi:   fi,
		from: from,
		to:   to,
	}
	if _, ok := s.peek(); !ok {
		if err := s.error(); err != io.EOF {
			return nil, err
		}
		return nil, nil
	}
	s.close()
	return s, nil
}

func (s *fileLogStream) close() {
	s.f.Close()
	s.f = nil
	s.d = nil
}

func (s *fileLogStream) open() bool {
	const readBufSize = 1024
	if s.f, s.err = os.Open(s.fi.path); s.err != nil {
		return false
	}
	if s.err = seekToFirstAfterFrom(s.f, s.from); s.err != nil {
		return false
	}
	s.d = log.NewEntryDecoder(bufio.NewReaderSize(s.f, readBufSize))
	return true
}

func (s *fileLogStream) peek() (log.Entry, bool) {
	for !s.read && s.err == nil {
		justOpened := false
		if s.d == nil {
			if !s.open() {
				return log.Entry{}, false
			}
			justOpened = true
		}
		var e log.Entry
		if s.err = s.d.Decode(&e); s.err != nil {
			s.close()
			s.e = log.Entry{}
			break
		}
		// Upon re-opening the file, we'll read s.e again.
		if justOpened && e == s.e {
			continue
		}
		s.e = e
		if s.e.Time < s.prevTime {
			s.e.Time = s.prevTime
		} else {
			s.prevTime = s.e.Time
		}
		afterTo := !s.to.IsZero() && s.e.Time > s.to.UnixNano()
		if afterTo {
			s.close()
			s.e = log.Entry{}
			s.err = io.EOF
		} else {
			beforeFrom := !s.from.IsZero() && s.e.Time < s.from.UnixNano()
			s.read = !beforeFrom
		}
	}
	return s.e, s.err == nil
}

func (s *fileLogStream) pop() (e log.Entry, ok bool) {
	if e, ok = s.peek(); !ok {
		return
	}
	s.read = false
	return e, ok
}

func (s *fileLogStream) fileInfo() *fileInfo { return &s.fi }
func (s *fileLogStream) error() error        { return s.err }

// seekToFirstAfterFrom uses binary search to seek to an offset after all
// entries which occur before from.
func seekToFirstAfterFrom(f *os.File, from time.Time) (err error) {
	if from.IsZero() {
		return nil
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	offset := sort.Search(int(size), func(i int) bool {
		if _, err := f.Seek(int64(i), io.SeekStart); err != nil {
			panic(err)
		}
		var e log.Entry
		err := log.NewEntryDecoder(f).Decode(&e)
		if err != nil {
			if err == io.EOF {
				return true
			}
			panic(err)
		}
		return e.Time >= from.UnixNano()
	})
	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	var e log.Entry
	if err := log.NewEntryDecoder(f).Decode(&e); err != nil {
		return err
	}
	_, err = f.Seek(int64(offset), io.SeekStart)
	return err
}
