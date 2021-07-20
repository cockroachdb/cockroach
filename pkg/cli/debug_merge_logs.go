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
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"golang.org/x/sync/errgroup"
)

type logStream interface {
	fileInfo() *fileInfo // FileInfo for the current entry available in peek.
	peek() (logpb.Entry, bool)
	pop() (logpb.Entry, bool) // If called after peek, must return the same values.
	error() error
}

// writeLogStream pops messages off of s and writes them to out prepending
// prefix per message and filtering messages which match filter.
func writeLogStream(
	s logStream, out io.Writer, filter *regexp.Regexp, prefix string, keepRedactable bool,
) error {
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
		logpb.Entry
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
		return log.FormatLegacyEntry(ei.Entry, w)
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
	editMode log.EditSensitiveData,
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
	return newMergedStream(ctx, files, from, to, editMode)
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
	ctx context.Context, files []fileInfo, from, to time.Time, editMode log.EditSensitiveData,
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
			s, err := newFileLogStream(files[i], from, to, editMode)
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

func (l *mergedStream) peek() (logpb.Entry, bool) {
	if len(*l) == 0 {
		return logpb.Entry{}, false
	}
	return (*l)[0].peek()
}

func (l *mergedStream) pop() (logpb.Entry, bool) {
	e, ok := l.peek()
	if !ok {
		return logpb.Entry{}, false
	}
	s := (*l)[0]
	s.pop()
	if _, stillOk := s.peek(); stillOk {
		heap.Push(l, heap.Pop(l))
	} else if err := s.error(); err != nil && err != io.EOF {
		return logpb.Entry{}, false
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
	if programGroup == 0 || programFilter == nil {
		programGroup = 0
	}
	to = to.Truncate(time.Second) // log files only have second resolution
	var files []fileInfo
	for _, p := range paths {
		// NB: come go1.16, we should use WalkDir here as it is more efficient.
		if err := filepath.Walk(p, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				// Don't act on the directory itself, Walk will visit it for us.
				return nil
			}
			// We're looking at a file.
			fi, ok := getLogFileInfo(p, filePattern)
			if !ok {
				return nil
			}
			if programGroup > 0 {
				program := fi.path[fi.matches[2*programGroup]:fi.matches[2*programGroup+1]]
				if !programFilter.MatchString(program) {
					return nil
				}
			}
			files = append(files, fi)
			return nil
		}); err != nil {
			return nil, err
		}
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
	e       logpb.Entry
	read    bool
	ok      bool
	c       chan logpb.Entry
	ctx     context.Context
	g       *errgroup.Group
}

func (bs *bufferedLogStream) run() {
	const readChanSize = 512
	bs.c = make(chan logpb.Entry, readChanSize)
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

func (bs *bufferedLogStream) peek() (logpb.Entry, bool) {
	if bs.ok && !bs.read {
		if bs.c == nil { // indicates that run has not been called
			bs.runOnce.Do(bs.run)
		}
		bs.e, bs.ok = <-bs.c
		bs.read = true
	}
	if !bs.ok {
		return logpb.Entry{}, false
	}
	return bs.e, true
}

func (bs *bufferedLogStream) pop() (logpb.Entry, bool) {
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
	editMode log.EditSensitiveData

	e   logpb.Entry
	err error
}

// newFileLogStream constructs a *fileLogStream and then peeks at the file to
// ensure that it contains a valid entry after from. If the file contains only
// data which precedes from, (nil, nil) is returned. If an io error is
// encountered during the initial peek, that error is returned. The underlying
// file is always closed before returning from this constructor so the initial
// peek does not consume resources.
func newFileLogStream(
	fi fileInfo, from, to time.Time, editMode log.EditSensitiveData,
) (logStream, error) {
	s := &fileLogStream{
		fi:       fi,
		from:     from,
		to:       to,
		editMode: editMode,
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
	if s.err = seekToFirstAfterFrom(s.f, s.from, s.editMode); s.err != nil {
		return false
	}
	s.d = log.NewEntryDecoder(bufio.NewReaderSize(s.f, readBufSize), s.editMode)
	return true
}

func (s *fileLogStream) peek() (logpb.Entry, bool) {
	for !s.read && s.err == nil {
		justOpened := false
		if s.d == nil {
			if !s.open() {
				return logpb.Entry{}, false
			}
			justOpened = true
		}
		var e logpb.Entry
		if s.err = s.d.Decode(&e); s.err != nil {
			s.close()
			s.e = logpb.Entry{}
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
			s.e = logpb.Entry{}
			s.err = io.EOF
		} else {
			beforeFrom := !s.from.IsZero() && s.e.Time < s.from.UnixNano()
			s.read = !beforeFrom
		}
	}
	return s.e, s.err == nil
}

func (s *fileLogStream) pop() (e logpb.Entry, ok bool) {
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
func seekToFirstAfterFrom(f *os.File, from time.Time, editMode log.EditSensitiveData) (err error) {
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
		var e logpb.Entry
		err := log.NewEntryDecoder(f, editMode).Decode(&e)
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
	var e logpb.Entry
	if err := log.NewEntryDecoder(f, editMode).Decode(&e); err != nil {
		return err
	}
	_, err = f.Seek(int64(offset), io.SeekStart)
	return err
}
