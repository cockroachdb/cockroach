// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgurl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

func TestURL(t *testing.T) {
	datadriven.RunTest(t, "testdata/url", func(t *testing.T, td *datadriven.TestData) string {
		var result bytes.Buffer

		var u *URL
		switch td.Cmd {
		case "insecure":
			u = New().WithInsecure()

		case "url":
			var err error
			u, err = Parse(td.Input)
			if err != nil {
				fmt.Fprintf(&result, "parse error: %v", err)
				return result.String()
			}

		default:
			t.Fatalf("unrecognized command: %s", td.Cmd)
		}

		if err := u.Validate(); err != nil {
			fmt.Fprintf(&result, "%v\nDetails:\n%s----\n", err, errors.FlattenDetails(err))
		}

		fmt.Fprintf(&result, "pq URL: %s\n", u.ToPQ())
		fmt.Fprintf(&result, "DSN:    %s\n", u.ToDSN())
		fmt.Fprintf(&result, "JDBC:   %s\n", u.ToJDBC())

		u.
			WithDefaultUsername("defaultuser").
			WithDefaultDatabase("defaultdb").
			WithDefaultHost("defaulthost").
			WithDefaultPort("26257")
		fmt.Fprintln(&result, "--defaults filled--")
		fmt.Fprintf(&result, "pq URL: %s\n", u.ToPQ())
		fmt.Fprintf(&result, "DSN:    %s\n", u.ToDSN())
		fmt.Fprintf(&result, "JDBC:   %s\n", u.ToJDBC())

		return result.String()
	})
}
