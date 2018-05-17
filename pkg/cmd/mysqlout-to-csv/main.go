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

package main

import (
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util"
)

var (
	columns = flag.Int("columns", 0, "check that N columns are actually decoded")

	lineSep     = flag.String("lines-terminated-by", "\n", "line separator character")
	fieldsSep   = flag.String("fields-terminated-by", "\t", "field separator character")
	encloseChar = flag.String("fields-enclosed-by", "", "field enclosing character")
	escapeChar  = flag.String("fields-escaped-by", "\\", "escape character")
	hexColList  = flag.String("hex-cols", "", "list of columns (0 indexed) to hex encode")

	chunkCSVRows = flag.Int("chunk-rows", 10000, "chunk csv into N rows per file (0 no chunking)")

	progressFreq = flag.Int("progress", 10000, "number of rows per progress update (0 for no updates)")
)

func runeFromString(s string) rune {
	if s == "" {
		return 0
	}
	r, err := util.GetSingleRune(s)
	if err != nil {
		panic(err)
	}
	return r
}

func main() {
	flag.Parse()

	if flags := len(flag.Args()); flags != 2 {
		fmt.Printf("usage: %s <mysql outfile> <csv>\n", os.Args[0])
		os.Exit(1)
	}
	var hexCols []int
	for _, s := range strings.Split(*hexColList, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			i, err := strconv.Atoi(s)
			if err != nil {
				panic("cannot parse column id in hex list")
			}
			hexCols = append(hexCols, i)
		}
	}

	in, err := os.Open(flag.Arg(0))
	if err != nil {
		panic(err)
	}

	dest := flag.Arg(1)

	d := dumpReader{
		width:          *columns,
		fieldSep:       runeFromString(*fieldsSep),
		rowSep:         runeFromString(*lineSep),
		hasEncloseChar: *encloseChar != "",
		encloseChar:    runeFromString(*encloseChar),
		hasEscapeChar:  *escapeChar != "",
		escapeChar:     runeFromString(*escapeChar),
	}

	chunkSize := *chunkCSVRows

	var first bool
	var totalRows, chunkRows, chunks int

	var outPath string
	var out io.WriteCloser
	var writer *csv.Writer

	openOutput := func() {
		first = true
		outPath = dest
		if chunkSize > 0 {
			chunks++
			outPath = fmt.Sprintf("%s.%d", dest, chunks)
		}
		o, err := os.Create(outPath)
		if err != nil {
			panic(err)
		}
		out = o
		writer = csv.NewWriter(out)
		chunkRows = 0
	}

	d.f = func(r []string) error {
		for _, i := range hexCols {
			r[i] = hex.EncodeToString([]byte(r[i]))
		}
		err := writer.Write(r)
		if err != nil {
			return err
		}
		chunkRows++
		totalRows++
		if first {
			fmt.Printf("\nWriting %d column csv to %s...\n", len(r), outPath)
			first = false
		}
		if p := *progressFreq; p > 0 && totalRows%p == 0 {
			fmt.Printf("\rWrote %d rows...", totalRows)
		}
		if chunkSize > 0 && chunkRows > chunkSize {
			writer.Flush()
			out.Close()
			openOutput()
		}
		return nil
	}

	openOutput()
	if err := d.Process(in); err != nil {
		panic(err)
	}
	writer.Flush()
	out.Close()

	fmt.Printf("\rWrote %d rows.\n", totalRows)
}
