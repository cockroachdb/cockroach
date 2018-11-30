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
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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

// mergedStream is a heap of log streams.
type mergedStream []logStream

func newMergedStream(files []fileInfo, from, to time.Time) *mergedStream {
	// TODO: think about clock movement and PID

	var res mergedStream
	var curNode []fileInfo

	// Constructs a stream from the files of curNode and peeks for the current
	// timestamp.
	// TODO(ajwerner): could do the peek in parallel though most of the cost
	// has been mitigated with the binary search.
	addLogStream := func() {
		s := newNodeLogStream(curNode, from, to)
		if _, ok := s.peek(); ok {
			res = append(res, s)
		}
		curNode = nil
	}
	// Sort the files so that they are in host, then time ascending order to split
	// the files into streams per node.
	var prev *fileInfo
	sort.Sort(logFileInfoList(files))
	for i := range files {
		cur := &files[i]
		if prev != nil &&
			(prev.Details.Host != cur.Details.Host ||
				prev.Details.Program != cur.Details.Program) {
			addLogStream()
		}
		// Overwrite the previous file if there is one and the current file starts
		// before from.
		curIsBeforeFrom := !from.IsZero() && cur.Details.Time < from.UnixNano()
		if len(curNode) > 0 && curIsBeforeFrom {
			curNode[len(curNode)-1] = *cur
		} else {
			curNode = append(curNode, *cur)
		}
		prev = cur
	}
	if len(curNode) > 0 {
		addLogStream()
	}
	heap.Init(&res)
	return &res
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

type fileInfo struct {
	path string
	log.FileInfo
}

type logFileInfoList []fileInfo

func (l logFileInfoList) Len() int      { return len(l) }
func (l logFileInfoList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l logFileInfoList) Less(i, j int) bool {
	if l[i].Details.Host == l[j].Details.Host {
		return l[i].Details.Time < l[j].Details.Time
	}
	return l[i].Details.Host < l[j].Details.Host
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
		FileInfo: log.NewFileInfo(details, fi),
	}, nil
}

func findLogFiles(paths []string, program *regexp.Regexp, to time.Time) ([]fileInfo, error) {
	fileChan := make(chan fileInfo, len(paths))
	var g errgroup.Group
	to = to.Truncate(time.Second) // Filenames only have second resolution.
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

func newLogStreamFromPatterns(
	patterns []string, program *regexp.Regexp, from, to time.Time,
) (logStream, error) {
	paths, err := expandPatterns(patterns)
	if err != nil {
		return nil, err
	}
	files, err := findLogFiles(paths, program, to)
	if err != nil {
		return nil, err
	}
	return newMergedStream(files, from, to), nil
}

func newNodeLogStream(files []fileInfo, from, to time.Time) logStream {
	return &nodeStream{
		files: files,
		from:  from,
		to:    to,
	}
}

type nodeStream struct {
	from, to time.Time
	prevTime int64
	cur      *os.File
	files    []fileInfo
	d        *log.EntryDecoder

	read bool

	e   log.Entry
	err error
}

func (s *nodeStream) peek() (log.Entry, bool) {
	for !s.read && s.err == nil {
		if s.d == nil && !rotateFile(s) {
			return log.Entry{}, false
		}
		if s.err = s.d.Decode(&s.e); s.err != nil {
			s.cur.Close()
			s.cur = nil
			s.d = nil
			s.files = s.files[1:]
			if s.err == io.EOF {
				s.err = nil
				continue
			}
			s.e = log.Entry{}
		}
		if s.e.Time < s.prevTime {
			s.e.Time = s.prevTime
		} else {
			s.prevTime = s.e.Time
		}
		afterTo := !s.to.IsZero() && s.e.Time > s.to.UnixNano()
		if afterTo {
			s.cur.Close()
			s.cur = nil
			s.d = nil
			s.files = nil
		} else {
			beforeFrom := !s.from.IsZero() && s.e.Time < s.from.UnixNano()
			s.read = !beforeFrom
		}
	}
	return s.e, s.err == nil
}

func (s *nodeStream) pop() (e log.Entry, ok bool) {
	if e, ok = s.peek(); !ok {
		return
	}
	s.read = false
	return e, ok
}

func (s *nodeStream) fileInfo() *log.FileInfo {
	return &s.files[0].FileInfo
}

func (s *nodeStream) error() error { return s.err }

func rotateFile(s *nodeStream) bool {
	if len(s.files) == 0 {
		return false
	}
	if s.cur, s.err = os.Open(s.files[0].path); s.err != nil {
		return false
	}
	if s.err = seekToFirstAfterFrom(s.cur, s.from); s.err != nil {
		return false
	}
	s.d = log.NewEntryDecoder(s.cur)
	return true
}

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
		if _, err := f.Seek(int64(i), os.SEEK_SET); err != nil {
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
	if _, err := f.Seek(int64(offset), os.SEEK_SET); err != nil {
		return err
	}
	var e log.Entry
	if err := log.NewEntryDecoder(f).Decode(&e); err != nil {
		return err
	}
	_, err = f.Seek(int64(offset), os.SEEK_SET)
	return err
}
