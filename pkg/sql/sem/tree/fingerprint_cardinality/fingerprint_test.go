// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fingerprint_cardinality

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/stretchr/testify/require"
)

const (
	testInputs = "testdata/stmt_inputs.txt"
)

type entry struct {
	Stmt string `json:"stmt"`
	DB   string `json:"db"`
}

// TestGenInputData is used to clean SampledQuery telemetry data sourced from Splunk in preparation for cardinality
// testing, with a focus on statement fingerprinting. As one works to improve our statement fingerprinting algorithm,
// these tests can be used to estimate how impactful a change would be against an actual customer workload.
//
// The input file to TestGenInputData must satisfy a few invariants:
//  1. Each line of the input file must contain a complete JSON object
//  2. Each line's object must be of the format {.."result":{"event.Statement":"...","event.Database":"..."}}
//     - NB: this is the downloaded JSON format from Splunk
//
// A recommended Splunk query to use for generating this data is:
//
//	index=* friendly_id="<YOUR_FRIENDLY_ID>" |
//	search channel=TELEMETRY "event.EventType"=sampled_query |
//	dedup event.StatementFingerprintID |
//	table event.Statement, event.Database
//
// You can save the results as a JSON file.
//
// Once you have your downloaded file, move it to pkg/sql/sem/tree/fingerprintingbench/testdata. You can then add the
// filepath (starting with testdata/) to the cleanInputData call below. The output will be written to cleanedOutputFile,
// which will be used as input to TestStmtFingerprintCardinality.
//
// NB: The ways in which we clean the input data are not perfect, and we've encountered instances where the cleaned
// stmts are unable to be parsed to an AST. However, in most cases, the cleaned result works as expected. If you run
// into a scenario where a cleaned stmt is unable to be parsed, consider expanding upon the cleanStmtString function
// to make it more resilient.
//
// // We mark this test as skipped so that it doesn't run during the CI process.
func TestGenInputData(t *testing.T) {
	t.Skip("used for manual experimentation")
	require.NoError(t, cleanInputData([]string{ /* ADD INPUT FILEPATH HERE */ }, testInputs))
}

// TestStmtFingerprintCardinality uses the output of TestGenInputData as its input, and for each stmt within, generates
// a stmt fingerprint using both the old and new tree.FmtFlags. At the end, both the old and new cardinality counts are
// logged for comparison.
//
// You can optionally set writeResultsToFile in each individual test case to true if you'd like to analyze the processed
// results from either test run, which will be written to a file in the testdata directory.
//
// We mark this test as skipped so that it doesn't run during the CI process.
func TestStmtFingerprintCardinality(t *testing.T) {
	t.Skip("used for manual experimentation")
	inputs, err := loadTestInputs()
	require.NoError(t, err)

	type testCase struct {
		name               string
		fmtBitMask         tree.FmtFlags
		writeResultsToFile bool
		done               func(int)
	}
	oldSetSize := 0
	newSetSize := 0
	for _, tc := range []testCase{
		{
			name:               "legacy_fingerprinting",
			fmtBitMask:         tree.FmtHideConstants,
			writeResultsToFile: false,
			done:               func(size int) { oldSetSize = size },
		},
		{
			name:               "new_fingerprinting",
			fmtBitMask:         tree.FmtForFingerprint,
			writeResultsToFile: false,
			done:               func(size int) { newSetSize = size },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fingerprintSet := map[appstatspb.StmtFingerprintID]string{}
			for _, input := range inputs {
				parsed, err := parser.ParseOne(input.Stmt)
				require.NoError(t, err)
				stmtNoConstants := tree.AsStringWithFlags(parsed.AST, tc.fmtBitMask)
				fingerprintID := appstatspb.ConstructStatementFingerprintID(
					stmtNoConstants, false /* failed */, false /* implicitTxn */, input.DB)
				if _, ok := fingerprintSet[fingerprintID]; !ok {
					fingerprintSet[fingerprintID] = stmtNoConstants
				}
			}
			tc.done(len(fingerprintSet))
			if tc.writeResultsToFile {
				outputFile, err := os.Create(fmt.Sprintf("testdata/%s.json", tc.name))
				require.NoError(t, err)
				defer outputFile.Close()
				for fingerprint, stmt := range fingerprintSet {
					_, err := outputFile.WriteString(fmt.Sprintf(
						`{"stmtFingerprintID":%q,"stmt":%q}`,
						hex.EncodeToString(sqlstatsutil.EncodeUint64ToBytes(uint64(fingerprint))),
						stmt))
					require.NoError(t, err)
					_, err = outputFile.WriteString("\n")
					require.NoError(t, err)
				}
			}
		})
	}
	t.Logf("old set size: %d", oldSetSize)
	t.Logf("new set size: %d", newSetSize)
}

func loadTestInputs() ([]entry, error) {
	output := make([]entry, 0)
	inputFile, err := os.Open(testInputs)
	if err != nil {
		return output, err
	}
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		e := entry{}
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return output, err
		}
		output = append(output, e)
	}
	return output, nil
}

func cleanInputData(inputFilenames []string, outputFilename string) error {
	type inputBody struct {
		Stmt string `json:"event.Statement"`
		Db   string `json:"event.Database"`
	}
	type inputEntry struct {
		Res inputBody `json:"result"`
	}

	outputFile, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	for _, inputFilename := range inputFilenames {
		inputFile, err := os.Open(inputFilename)
		if err != nil {
			return err
		}
		defer inputFile.Close()

		scanner := bufio.NewScanner(inputFile)
		o := entry{}
		for scanner.Scan() {
			e := inputEntry{}
			if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
				return err
			}
			cleanedStmt := cleanStmtString(e.Res.Stmt, e.Res.Db)
			o.Stmt = cleanedStmt
			o.DB = e.Res.Db
			outputBytes, err := json.Marshal(&o)
			if err != nil {
				return err
			}
			if _, err := outputFile.Write(outputBytes); err != nil {
				return err
			}
			if _, err := outputFile.WriteString("\n"); err != nil {
				return err
			}
		}
	}
	return nil
}

func cleanStmtString(stmt string, db string) string {
	s := addDatabaseNameToStmt(stmt, db)
	s = replaceRedactionMarkers(s)
	return s
}

func addDatabaseNameToStmt(stmt string, database string) string {
	return strings.ReplaceAll(stmt, `""."".`, `"`+database+`".`)
}

func replaceRedactionMarkers(stmt string) string {
	replacementOpts := []string{
		"$%d",
		"'_'",
		"_",
	}
	stmt = strings.ReplaceAll(stmt, "IS ‹×›", replacementOpts[1])
	argNum := 1
	for strings.Contains(stmt, "‹×›") {
		replacement := replacementOpts[rand.Intn(len(replacementOpts))]
		if replacement == replacementOpts[0] {
			replacement = fmt.Sprintf(replacement, argNum)
			argNum++
		}
		stmt = strings.Replace(stmt, "‹×›", replacement, 1)
	}
	return stmt
}
