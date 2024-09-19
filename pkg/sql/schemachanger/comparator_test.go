// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachanger_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

var _ sctest.StmtLineReader = (*staticSQLStmtLineProvider)(nil)

// staticSQLStmtLineProvider implements sctest.StmtLineReader.
type staticSQLStmtLineProvider struct {
	stmts []string
	next  int
}

func (ss *staticSQLStmtLineProvider) HasNextLine() bool {
	return ss.next != len(ss.stmts)
}

func (ss *staticSQLStmtLineProvider) NextLine() string {
	ss.next++
	return ss.stmts[ss.next-1]
}

func runSchemaChangeComparatorTest(t *testing.T, logicTestFile string) {
	defer log.Scope(t).Close(t)
	var path string
	if bazel.BuiltWithBazel() {
		var err error
		path, err = bazel.Runfile(logicTestFile)
		require.NoError(t, err)
	} else {
		path = "../../../" + logicTestFile
	}
	basename := filepath.Base(path)

	var skipComparatorTest = envutil.EnvOrDefaultBool("COCKROACH_SCHEMA_CHANGE_COMPARATOR_SKIP", true)
	if skipComparatorTest {
		skip.IgnoreLint(t, "requires COCKROACH_SCHEMA_CHANGE_COMPARATOR_SKIP to be set")
	}

	if skipEntry, skipReason := shouldSkipLogicTestCorpusEntry(basename); skipEntry {
		skip.IgnoreLint(t, skipReason)
	}

	stmts, err := collectStmtsFrom(path)
	require.NoError(t, err)

	ss := &staticSQLStmtLineProvider{
		stmts: stmts,
	}
	sctest.CompareLegacyAndDeclarative(t, ss)
}

// shouldSkipLogicTestCorpusEntry is the place where we blacklist entries in the
// logictest stmts corpus. Each blacklisted entry should be justified with
// comments.
func shouldSkipLogicTestCorpusEntry(entryName string) (skip bool, skipReason string) {
	switch entryName {
	case "crdb_internal":
		// `crdb_internal` contains stmts like `SELECT crdb_internal.force_panic('foo')`
		// that will cause the framework to crash. Also, in general, we don't care about
		// crdb_internal functions for purpose of schema changer comparator testing.
		return true, `"crdb_internal" contains statement like "crdb_internal.force_panic()" that would crash the testing framework`
	case "schema_repair":
		// `schema_repair` contains stmts like `SELECT crdb_internal.unsafe_delete_descriptor(id)`
		// that will corrupt descriptors. This subsequently would fail the query that
		// attempts to fetch all descriptors during the post-execution metadata
		// identity check.
		return true, `"schema_repair" contains statement like "crdb_internal.unsafe_delete_descriptor(id)" that would cause descriptor corruptions, which would subsequently fail the query to fetch descriptors during the metadata identity check`
	default:
		return false, ""
	}
}

// collectStmtsFrom collects statements from a logic test file so that they
// can be used by the schema change comparator tests.
func collectStmtsFrom(path string) (stmts []string, err error) {
	logicTestFile, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer logicTestFile.Close()

	// Logictest framework initializes the cluster with a database `test` and an
	// user `testuser`.
	stmts = append(stmts, "CREATE DATABASE IF NOT EXISTS test;")
	stmts = append(stmts, "CREATE USER testuser;")

	// Collect statements from logic test `inPath` into `stmts`.
	s := bufio.NewScanner(logicTestFile)
	for s.Scan() {
		line := s.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		var stmt string
		switch cmd {
		case "statement":
			stmt = readLinesUntilSeparatorLine(s, false /* is4DashesSepLine */)
		case "query":
			stmt = readLinesUntilSeparatorLine(s, true /* is4DashesSepLine */)
		}
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}

	return stmts, nil
}

// Accumulate lines until we hit a "separator line".
//   - An empty line is always a separator line.
//   - If `is4DashesSepLine` is true, a line of "----" is also considered a separator line.
func readLinesUntilSeparatorLine(s *bufio.Scanner, is4DashesSepLine bool) string {
	isSepLine := func(line string) bool {
		if line == "" || (is4DashesSepLine && line == "----") {
			return true
		}
		return false
	}

	var sb strings.Builder
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if isSepLine(line) {
			break
		}
		sb.WriteString(line + "\n")
	}

	return sb.String()
}
