// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/docgen/extract"
	"github.com/stretchr/testify/require"
)

// TestStmtSpecs ensures that each statement in sql.y identified by
// %type <tree.Statement> has a corresponding entry in the stmtSpec slice
// returned by getAllStmtSpecs().
// Statements that are specified to be skipped, unimplemented or have no
// branches should be ignored.
func TestStmtSpecs(t *testing.T) {
	sqlGrammarFile := "../../sql/parser/sql.y"
	// runBNF with a local file does not use the API.
	bnfAPITimeout := time.Duration(0)

	file, err := os.Open(sqlGrammarFile)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	bnf, err := runBNF(sqlGrammarFile, bnfAPITimeout)
	require.NoError(t, err)

	br := func() io.Reader {
		return bytes.NewReader(bnf)
	}

	grammar, err := extract.ParseGrammar(br())
	require.NoError(t, err)

	sqlStmts := make(map[string]bool)

	stmtRegex, err := regexp.Compile(`%type <tree.Statement>`)
	require.NoError(t, err)
	for scanner.Scan() {
		text := scanner.Text()
		if stmtRegex.MatchString(text) {
			// Get just the statement name after the "%type <tree.Statement>".
			stmt := strings.Split(text, "%type <tree.Statement> ")[1]

			// If the stmt does not in appear in grammar, it should not be
			// documented. It either has no branches or is specified to be skipped.
			if _, found := grammar[stmt]; !found {
				continue
			}

			sqlStmts[stmt] = false
		}
	}

	stmtSpecs, err := getAllStmtSpecs(sqlGrammarFile, -1)
	require.NoError(t, err)

	for _, spec := range stmtSpecs {
		sqlStmts[spec.GetStatement()] = true
	}

	for stmt, found := range sqlStmts {
		if !found {
			t.Error(fmt.Sprintf("%s defined as a statement "+
				"in sql.y but not found in diagrams.go getAllStmtSpecs.", stmt))
		}
	}
}
