// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package cli

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
)

type logStream interface {
	fileInfo() *log.FileInfo // FileInfo for the current entry available in peek.
	peek() (log.Entry, bool)
	pop() (log.Entry, bool) // If called after peek, must return the same values.
	error() error
}

// writeLogStream pops messages off of s and writes them to out prepending
// prefix per message and filtering messages which match filter.
func writeLogStream(
	s logStream, out io.Writer, filter *regexp.Regexp, prefix *template.Template,
) error {
	const chanSize = 1 << 16        // 64k
	const maxWriteBufSize = 1 << 18 // 256kB

	prefixCache := map[*log.FileInfo][]byte{}
	getPrefix := func(fi *log.FileInfo) ([]byte, error) {
		if prefixBuf, ok := prefixCache[fi]; ok {
			return prefixBuf, nil
		}
		var prefixBuf bytes.Buffer
		if err := prefix.Execute(&prefixBuf, fi); err != nil {
			return nil, err
		}
		prefixCache[fi] = prefixBuf.Bytes()
		return prefixCache[fi], nil
	}

	type entryInfo struct {
		log.Entry
		*log.FileInfo
	}
	render := func(ei entryInfo, w io.Writer) (err error) {
		var prefixBytes []byte
		if prefixBytes, err = getPrefix(ei.FileInfo); err != nil {
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
			case entryChan <- entryInfo{Entry: e, FileInfo: s.fileInfo()}:
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
				if filter != nil && !filter.Match(pending.Bytes()[startLen:]) {
					pending.Truncate(startLen)
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
// log filename pattern and match the program. The returned stream will only
// return log entries in [from, to].
func newMergedStreamFromPatterns(
	ctx context.Context, patterns []string, program *regexp.Regexp, from, to time.Time,
) (logStream, error) {
	paths, err := expandPatterns(patterns)
	if err != nil {
		return nil, err
	}
	files, err := findLogFiles(paths, program, to)
	if err != nil {
		return nil, err
	}
	return newMergedStream(ctx, files, from, to)
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

func (l *mergedStream) fileInfo() *log.FileInfo {
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

type parseLogFilenameError struct {
	path string
	err  error
}

func (e *parseLogFilenameError) Error() string {
	return fmt.Sprintf("failed to parse filename for %v: %v", e.path, e.err)
}

func getLogFileInfo(path string) (fileInfo, error) {
	filename := filepath.Base(path)
	details, err := log.ParseLogFilename(filename)
	if err != nil {
		return fileInfo{}, &parseLogFilenameError{path: path, err: err}
	}
	fi, err := os.Stat(path)
	if err != nil {
		return fileInfo{}, err
	}
	return fileInfo{
		path:     path,
		FileInfo: log.MakeFileInfo(details, fi),
	}, nil
}

type fileInfo struct {
	path string
	log.FileInfo
}

func findLogFiles(paths []string, program *regexp.Regexp, to time.Time) ([]fileInfo, error) {
	to = to.Truncate(time.Second) // Log files only have second resolution.
	fileChan := make(chan fileInfo, len(paths))
	var g errgroup.Group
	for i := range paths {
		p := paths[i]
		g.Go(func() error {
			fi, err := getLogFileInfo(p)
			if err == nil && program.MatchString(fi.Details.Program) {
				if to.IsZero() || timeutil.Unix(0, fi.Details.Time).Before(to) {
					fileChan <- fi
				}
			} else if _, isParseErr := err.(*parseLogFilenameError); isParseErr {
				err = nil
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
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
		if bs.c == nil { // Indicates that run has not been called.
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
	// we want to create the struct, do a peek, then close the file
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

func (s *fileLogStream) fileInfo() *log.FileInfo { return &s.fi.FileInfo }
func (s *fileLogStream) error() error            { return s.err }

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
		switch err := log.NewEntryDecoder(f).Decode(&e); err {
		case nil:
			return e.Time >= from.UnixNano()
		default:
			return true
		}
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
