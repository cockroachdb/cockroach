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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDatabaseDumpCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	tests := []struct {
		name   string
		create string
	}{
		{
			name: "columnsless_table",
			create: `
	CREATE DATABASE bar;
	USE bar;
	CREATE TABLE foo ();
	`,
		},
		{
			name: "table_with_columns",
			create: `
	CREATE DATABASE bar;
	USE bar;
	CREATE TABLE foo (id int primary key, text string not null);
	`,
		},
		{
			name: "autogenerate_hidden_colum",
			create: `
	CREATE DATABASE bar;
	USE bar;
	CREATE TABLE foo(id int);
	`,
		},
		{
			name: "columns_less_table_with_data",
			create: `
	CREATE DATABASE bar;
	USE bar;
	CREATE TABLE foo(id int);
	INSERT INTO foo(id) VALUES(1);
	INSERT INTO foo(id) VALUES(2);
	INSERT INTO foo(id) VALUES(3);
	ALTER TABLE foo DROP COLUMN id;
	`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newCLITest(cliTestParams{t: t})
			c.omitArgs = true
			defer c.cleanup()

			_, err := c.RunWithCaptureArgs([]string{"sql", "-e", test.create})
			require.NoError(t, err)

			dump, err := c.RunWithCaptureArgs([]string{"dump", "bar", "--dump-mode=schema"})
			require.NoError(t, err)

			// Run dump again and compare the outputs.
			_, err = c.RunWithCaptureArgs([]string{"sql", "-e", fmt.Sprintf(`CREATE DATABASE TEST;
USE TEST;
%s`, dump)})
			require.NoError(t, err)

			recreatedDump, err := c.RunWithCaptureArgs([]string{"dump", "TEST", "--dump-mode=schema"})
			require.NoError(t, err)

			require.Equal(t, dump, recreatedDump)
		})
	}
}

func TestDumpInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	tests := []struct {
		name     string
		cmd      []string
		expected string
	}{
		{
			name:     "dump",
			cmd:      []string{"dump"},
			expected: "dump\nERROR: accepts 1 arg(s), received 0\n",
		},
		{
			name:     "dump-bar",
			cmd:      []string{"dump", "bar"},
			expected: "dump bar\nERROR: only cockroach dump --dump-mode=schema is supported\n",
		},
		{
			name:     "dump-data",
			cmd:      []string{"dump", "bar", "--dump-mode=data"},
			expected: "dump bar --dump-mode=data\nERROR: invalid argument \"data\" for \"--dump-mode\" flag: invalid value for --dump-mode: data\n",
		},
		{
			name:     "dump-both",
			cmd:      []string{"dump", "bar", "--dump-mode=both"},
			expected: "dump bar --dump-mode=both\nERROR: invalid argument \"both\" for \"--dump-mode\" flag: invalid value for --dump-mode: both\n",
		},
		{
			name:     "dump-db-table",
			cmd:      []string{"dump", "dt", "t"},
			expected: "dump dt t\nERROR: accepts 1 arg(s), received 2\n",
		},
		{
			name:     "dump-db-table",
			cmd:      []string{"dump", "dt", "t", "--dump-mode=schema"},
			expected: "dump dt t --dump-mode=schema\nERROR: accepts 1 arg(s), received 2\n",
		},
	}

	for _, test := range tests {
		out, err := c.RunWithCaptureArgs(test.cmd)
		require.NoError(t, err)

		require.Equal(t, test.expected, out)
	}
}
