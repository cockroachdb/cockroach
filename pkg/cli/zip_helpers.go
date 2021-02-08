// Copyright 2021 The Cockroach Authors.
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
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// zipper is the interface to the zip file stored on disk.
type zipper struct {
	// zipper implements Mutex because it's not possible for multiple
	// goroutines to write concurrently to a zip.Writer.
	syncutil.Mutex

	f *os.File
	z *zip.Writer
}

func newZipper(f *os.File) *zipper {
	return &zipper{
		f: f,
		z: zip.NewWriter(f),
	}
}

func (z *zipper) close() error {
	z.Lock()
	defer z.Unlock()

	err1 := z.z.Close()
	err2 := z.f.Close()
	return errors.CombineErrors(err1, err2)
}

// createLocked opens a new entry in the zip file. The caller is
// responsible for locking the zipper beforehand.
// Unsafe for concurrent use otherwise.
func (z *zipper) createLocked(name string, mtime time.Time) (io.Writer, error) {
	fmt.Printf("writing: %s\n", name)
	if mtime.IsZero() {
		mtime = timeutil.Now()
	}
	return z.z.CreateHeader(&zip.FileHeader{
		Name:     name,
		Method:   zip.Deflate,
		Modified: mtime,
	})
}

// createRaw creates an entry and writes its contents as a byte slice.
// Safe for concurrent use.
func (z *zipper) createRaw(name string, b []byte) error {
	z.Lock()
	defer z.Unlock()

	w, err := z.createLocked(name, time.Time{})
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// createJSON creates an entry and writes its contents from a struct payload, converted to JSON.
// Safe for concurrent use.
func (z *zipper) createJSON(name string, m interface{}) error {
	if !strings.HasSuffix(name, ".json") {
		return errors.Errorf("%s does not have .json suffix", name)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return z.createRaw(name, b)
}

// createError reports an error payload.
// Safe for concurrent use.
func (z *zipper) createError(name string, e error) error {
	z.Lock()
	defer z.Unlock()

	w, err := z.createLocked(name+".err.txt", time.Time{})
	if err != nil {
		return err
	}
	fmt.Printf("  ^- resulted in %s\n", e)
	fmt.Fprintf(w, "%s\n", e)
	return nil
}

// createJSONOrError calls either createError() or createJSON()
// depending on whether the error argument is nil.
// Safe for concurrent use.
func (z *zipper) createJSONOrError(name string, m interface{}, e error) error {
	if e != nil {
		return z.createError(name, e)
	}
	return z.createJSON(name, m)
}

// createJSONOrError calls either createError() or createRaw()
// depending on whether the error argument is nil.
// Safe for concurrent use.
func (z *zipper) createRawOrError(name string, b []byte, e error) error {
	if filepath.Ext(name) == "" {
		return errors.Errorf("%s has no extension", name)
	}
	if e != nil {
		return z.createError(name, e)
	}
	return z.createRaw(name, b)
}

// fileNameEscaper is used to generate file names when the name of the
// file is derived from a SQL identifier or other stored data. This is
// necessary because not all characters in SQL identifiers and strings
// can be used in file names.
type fileNameEscaper struct {
	counters map[string]int
}

// escape ensures that f is stripped of characters that
// may be invalid in file names. The characters are also lowercased
// to ensure proper normalization in case-insensitive filesystems.
func (fne *fileNameEscaper) escape(f string) string {
	f = strings.ToLower(f)
	var out strings.Builder
	for _, c := range f {
		if c < 127 && (unicode.IsLetter(c) || unicode.IsDigit(c)) {
			out.WriteRune(c)
		} else {
			out.WriteByte('_')
		}
	}
	objName := out.String()
	result := objName

	if fne.counters == nil {
		fne.counters = make(map[string]int)
	}
	cnt := fne.counters[objName]
	if cnt > 0 {
		result += fmt.Sprintf("-%d", cnt)
	}
	cnt++
	fne.counters[objName] = cnt
	return result
}

// nodeSelection is used to define a subset of the nodes on the command line.
type nodeSelection struct {
	inclusive     rangeSelection
	exclusive     rangeSelection
	includedCache map[int]struct{}
	excludedCache map[int]struct{}
}

func (n *nodeSelection) isIncluded(nodeID roachpb.NodeID) bool {
	// Avoid recomputing the maps on every call.
	if n.includedCache == nil {
		n.includedCache = n.inclusive.items()
	}
	if n.excludedCache == nil {
		n.excludedCache = n.exclusive.items()
	}

	// If the included cache is empty, then we're assuming the node is included.
	isIncluded := true
	if len(n.includedCache) > 0 {
		_, isIncluded = n.includedCache[int(nodeID)]
	}
	// Then filter out excluded IDs.
	if _, excluded := n.excludedCache[int(nodeID)]; excluded {
		isIncluded = false
	}
	return isIncluded
}

// rangeSelection enables the selection of multiple ranges of
// consecutive integers. Used in combination with the node selection
// to enable selecting ranges of node IDs.
type rangeSelection struct {
	input  string
	ranges []vrange
}

type vrange struct {
	a, b int
}

func (r *rangeSelection) String() string { return r.input }

func (r *rangeSelection) Type() string {
	return "a-b,c,d-e,..."
}

func (r *rangeSelection) Set(v string) error {
	r.input = v
	for _, rs := range strings.Split(v, ",") {
		var thisRange vrange
		if strings.Contains(rs, "-") {
			ab := strings.SplitN(rs, "-", 2)
			a, err := strconv.Atoi(ab[0])
			if err != nil {
				return err
			}
			b, err := strconv.Atoi(ab[1])
			if err != nil {
				return err
			}
			if b < a {
				return errors.New("invalid range")
			}
			thisRange = vrange{a, b}
		} else {
			a, err := strconv.Atoi(rs)
			if err != nil {
				return err
			}
			thisRange = vrange{a, a}
		}
		r.ranges = append(r.ranges, thisRange)
	}
	return nil
}

// items returns the values selected by the range selection.
func (r *rangeSelection) items() map[int]struct{} {
	s := map[int]struct{}{}
	for _, vr := range r.ranges {
		for i := vr.a; i <= vr.b; i++ {
			s[i] = struct{}{}
		}
	}
	return s
}
