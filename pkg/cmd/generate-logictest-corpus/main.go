// Copyright 2023 The Cockroach Authors.
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
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var outDir = flag.String("out-dir", "",
	"directory path in which corpus files of logic tests stmts are stored")

func collectLogicTestsStmts() error {
	err := createOutDirIfNotExists()
	if err != nil {
		return err
	}
	logicTestDir, err := getLogicTestDir()
	if err != nil {
		return err
	}

	corpus := scpb.LogicTestStmtsCorpus{}
	err = filepath.WalkDir(logicTestDir, func(inPath string, d fs.DirEntry, err error) error {
		if inPath == logicTestDir || err != nil || d.IsDir() {
			return err
		}
		stmts, err := collectCorpusEntryFrom(inPath)
		if err != nil {
			return err
		}
		corpus.Entries = append(corpus.Entries, &scpb.LogicTestStmtsCorpus_Entry{
			Name:       d.Name(),
			Statements: stmts,
		})
		return nil
	})
	if err != nil {
		return err
	}

	// Serialize corpus to output file.
	corpusFilePath := filepath.Join(*outDir, "logictest-stmts-corpus")
	corpusFile, err := os.OpenFile(corpusFilePath, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer corpusFile.Close()
	corpusBytes, err := protoutil.Marshal(&corpus)
	if err != nil {
		return err
	}
	_, err = corpusFile.Write(corpusBytes)
	if err != nil {
		return err
	}
	return nil
}

func createOutDirIfNotExists() error {
	if *outDir == "" {
		return errors.New("please specify a output directory for corpus files via `-out-dir=[some-output-directory]`")
	}
	return os.MkdirAll(*outDir, os.ModePerm)
}

func getLogicTestDir() (logicTestDir string, err error) {
	if bazel.BuiltWithBazel() {
		logicTestDir, err = bazel.Runfile("pkg/sql/logictest/testdata/logic_test")
		return logicTestDir, err
	}
	return "", errors.New("non-bazel builds not supported.")
}

func collectCorpusEntryFrom(inPath string) (stmts []string, err error) {
	logicTestFile, err := os.OpenFile(inPath, os.O_RDONLY, os.ModePerm)
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

func main() {
	flag.Parse()
	if err := collectLogicTestsStmts(); err != nil {
		panic(err)
	}
}
