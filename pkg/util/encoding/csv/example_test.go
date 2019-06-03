// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License.

// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

package csv_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func ExampleReader() {
	ctx := context.Background()
	in := `first_name,last_name,username
"Rob","Pike",rob
Ken,Thompson,ken
"Robert","Griesemer","gri"
`
	r := csv.NewReader(strings.NewReader(in))

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(ctx, err)
		}

		fmt.Println(record)
	}
	// Output:
	// [first_name last_name username]
	// [Rob Pike rob]
	// [Ken Thompson ken]
	// [Robert Griesemer gri]
}

// This example shows how csv.Reader can be configured to handle other
// types of CSV files.
func ExampleReader_options() {
	ctx := context.Background()
	in := `first_name;last_name;username
"Rob";"Pike";rob
# lines beginning with a # character are ignored
Ken;Thompson;ken
"Robert";"Griesemer";"gri"
`
	r := csv.NewReader(strings.NewReader(in))
	r.Comma = ';'
	r.Comment = '#'

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(ctx, err)
	}

	fmt.Print(records)
	// Output:
	// [[first_name last_name username] [Rob Pike rob] [Ken Thompson ken] [Robert Griesemer gri]]
}

func ExampleReader_ReadAll() {
	ctx := context.Background()
	in := `first_name,last_name,username
"Rob","Pike",rob
Ken,Thompson,ken
"Robert","Griesemer","gri"
`
	r := csv.NewReader(strings.NewReader(in))

	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(ctx, err)
	}

	fmt.Print(records)
	// Output:
	// [[first_name last_name username] [Rob Pike rob] [Ken Thompson ken] [Robert Griesemer gri]]
}

func ExampleWriter() {
	ctx := context.Background()
	records := [][]string{
		{"first_name", "last_name", "username"},
		{"Rob", "Pike", "rob"},
		{"Ken", "Thompson", "ken"},
		{"Robert", "Griesemer", "gri"},
	}

	w := csv.NewWriter(os.Stdout)

	for _, record := range records {
		if err := w.Write(record); err != nil {
			log.Fatalf(ctx, "error writing record to csv: %v\n", err)
		}
	}

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	if err := w.Error(); err != nil {
		log.Fatal(ctx, err)
	}
	// Output:
	// first_name,last_name,username
	// Rob,Pike,rob
	// Ken,Thompson,ken
	// Robert,Griesemer,gri
}

func ExampleWriter_WriteAll() {
	ctx := context.Background()
	records := [][]string{
		{"first_name", "last_name", "username"},
		{"Rob", "Pike", "rob"},
		{"Ken", "Thompson", "ken"},
		{"Robert", "Griesemer", "gri"},
	}

	w := csv.NewWriter(os.Stdout)
	if err := w.WriteAll(records); err != nil { // calls Flush internally
		log.Fatalf(ctx, "error writing csv: %v\n", err)
	}

	if err := w.Error(); err != nil {
		log.Fatalf(ctx, "error writing csv: %v\n", err)
	}
	// Output:
	// first_name,last_name,username
	// Rob,Pike,rob
	// Ken,Thompson,ken
	// Robert,Griesemer,gri
}
