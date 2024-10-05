// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
			log.Fatalf(ctx, "%v", err)
		}

		fmt.Println(record)
	}
	// Output:
	// [{first_name false} {last_name false} {username false}]
	// [{Rob true} {Pike true} {rob false}]
	// [{Ken false} {Thompson false} {ken false}]
	// [{Robert true} {Griesemer true} {gri true}]
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
		log.Fatalf(ctx, "%v", err)
	}

	for _, record := range records {
		fmt.Println(record)
	}
	// Output:
	// [{first_name false} {last_name false} {username false}]
	// [{Rob true} {Pike true} {rob false}]
	// [{Ken false} {Thompson false} {ken false}]
	// [{Robert true} {Griesemer true} {gri true}]
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
		log.Fatalf(ctx, "%v", err)
	}

	for _, record := range records {
		fmt.Println(record)
	}
	// Output:
	// [{first_name false} {last_name false} {username false}]
	// [{Rob true} {Pike true} {rob false}]
	// [{Ken false} {Thompson false} {ken false}]
	// [{Robert true} {Griesemer true} {gri true}]
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
		log.Fatalf(ctx, "%v", err)
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
