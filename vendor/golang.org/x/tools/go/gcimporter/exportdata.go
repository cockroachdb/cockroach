// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file implements FindExportData.

package gcimporter

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func readGopackHeader(r *bufio.Reader) (name string, size int, err error) {
	// See $GOROOT/include/ar.h.
	hdr := make([]byte, 16+12+6+6+8+10+2)
	_, err = io.ReadFull(r, hdr)
	if err != nil {
		return
	}
	// leave for debugging
	if false {
		fmt.Printf("header: %s", hdr)
	}
	s := strings.TrimSpace(string(hdr[16+12+6+6+8:][:10]))
	size, err = strconv.Atoi(s)
	if err != nil || hdr[len(hdr)-2] != '`' || hdr[len(hdr)-1] != '\n' {
		err = errors.New("invalid archive header")
		return
	}
	name = strings.TrimSpace(string(hdr[:16]))
	return
}

// FindExportData positions the reader r at the beginning of the
// export data section of an underlying GC-created object/archive
// file by reading from it. The reader must be positioned at the
// start of the file before calling this function.
//
func FindExportData(r *bufio.Reader) (err error) {
	// Read first line to make sure this is an object file.
	line, err := r.ReadSlice('\n')
	if err != nil {
		return
	}
	if string(line) == "!<arch>\n" {
		// Archive file. Scan to __.PKGDEF.
		var name string
		var size int
		if name, size, err = readGopackHeader(r); err != nil {
			return
		}

		// Optional leading __.GOSYMDEF or __.SYMDEF.
		// Read and discard.
		if name == "__.SYMDEF" || name == "__.GOSYMDEF" {
			const block = 4096
			tmp := make([]byte, block)
			for size > 0 {
				n := size
				if n > block {
					n = block
				}
				if _, err = io.ReadFull(r, tmp[:n]); err != nil {
					return
				}
				size -= n
			}

			if name, size, err = readGopackHeader(r); err != nil {
				return
			}
		}

		// First real entry should be __.PKGDEF.
		if name != "__.PKGDEF" {
			err = errors.New("go archive is missing __.PKGDEF")
			return
		}

		// Read first line of __.PKGDEF data, so that line
		// is once again the first line of the input.
		if line, err = r.ReadSlice('\n'); err != nil {
			return
		}
	}

	// Now at __.PKGDEF in archive or still at beginning of file.
	// Either way, line should begin with "go object ".
	if !strings.HasPrefix(string(line), "go object ") {
		err = errors.New("not a go object file")
		return
	}

	// Skip over object header to export data.
	// Begins after first line with $$.
	for line[0] != '$' {
		if line, err = r.ReadSlice('\n'); err != nil {
			return
		}
	}

	return
}
