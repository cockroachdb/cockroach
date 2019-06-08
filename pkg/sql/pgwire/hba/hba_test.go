// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package hba

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestParse(t *testing.T) {
	datadriven.RunTest(t, filepath.Join("testdata", "parse"), func(d *datadriven.TestData) string {
		conf, err := Parse(d.Input)
		if err != nil {
			return fmt.Sprintf("error: %v\n", err)
		}
		return conf.String()
	})
}

// TODO(mjibson): these are untested outside ccl +gss builds.
var _ = Entry.GetOption
var _ = Entry.GetOptions
