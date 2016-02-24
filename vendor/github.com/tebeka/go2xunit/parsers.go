package main

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
)

var (
	matchDatarace = regexp.MustCompile("^WARNING: DATA RACE$").MatchString
)

type LineScanner struct {
	*bufio.Scanner
	lnum int
}

func NewLineScanner(r io.Reader) *LineScanner {
	scan := bufio.NewScanner(r)
	return &LineScanner{scan, 0}
}

func (ls *LineScanner) Scan() bool {
	val := ls.Scanner.Scan()
	ls.lnum++
	return val
}

func (ls *LineScanner) Line() int {
	return ls.lnum
}

// hasDatarace checks if there's a data race warning in the line
func hasDatarace(lines []string) bool {
	for _, line := range lines {
		if matchDatarace(line) {
			return true
		}
	}
	return false
}

// gc_Parse parses output of "go test -gocheck.vv", returns a list of tests
// See data/gocheck.out for an example
func gc_Parse(rd io.Reader) ([]*Suite, error) {
	find_start := regexp.MustCompile(gc_startRE).FindStringSubmatch
	find_end := regexp.MustCompile(gc_endRE).FindStringSubmatch
	find_suite := regexp.MustCompile(gc_suiteRE).FindStringSubmatch

	scanner := NewLineScanner(rd)
	var suites = make([]*Suite, 0)
	var suiteName string
	var suite *Suite

	var testName string
	var out []string

	for scanner.Scan() {
		line := scanner.Text()

		tokens := find_start(line)
		if len(tokens) > 0 {
			if tokens[2] == "SetUpTest" || tokens[2] == "TearDownTest" {
				continue
			}
			if testName != "" {
				return nil, fmt.Errorf("%d: start in middle\n", scanner.Line())
			}
			suiteName = tokens[1]
			testName = tokens[2]
			out = []string{}
			continue
		}

		tokens = find_end(line)
		if len(tokens) > 0 {
			if tokens[3] == "SetUpTest" || tokens[3] == "TearDownTest" {
				continue
			}
			if testName == "" {
				return nil, fmt.Errorf("%d: orphan end", scanner.Line())
			}
			if (tokens[2] != suiteName) || (tokens[3] != testName) {
				return nil, fmt.Errorf("%d: suite/name mismatch", scanner.Line())
			}
			test := &Test{Name: testName}
			test.Message = strings.Join(out, "\n")
			test.Time = tokens[4]
			test.Failed = (tokens[1] == "FAIL") || (tokens[1] == "PANIC")
			test.Passed = (tokens[1] == "PASS")
			test.Skipped = (tokens[1] == "SKIP" || tokens[1] == "MISS")

			if suite == nil || suite.Name != suiteName {
				suite = &Suite{Name: suiteName}
				suites = append(suites, suite)
			}
			suite.Tests = append(suite.Tests, test)

			testName = ""
			suiteName = ""
			out = []string{}

			continue
		}

		// last "suite" is test summary
		tokens = find_suite(line)
		if tokens != nil {
			if suite == nil {
				suite = &Suite{Name: tokens[2], Status: tokens[1], Time: tokens[3]}
				suites = append(suites, suite)
			} else {
				suite.Status = tokens[1]
				suite.Time = tokens[3]
			}

			testName = ""
			suiteName = ""
			out = []string{}

			continue
		}

		if testName != "" {
			out = append(out, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return suites, nil
}

// gt_Parse parser output of gotest
func gt_Parse(rd io.Reader) ([]*Suite, error) {
	find_start := regexp.MustCompile(gt_startRE).FindStringSubmatch
	find_end := regexp.MustCompile(gt_endRE).FindStringSubmatch
	find_suite := regexp.MustCompile(gt_suiteRE).FindStringSubmatch
	is_nofiles := regexp.MustCompile(gt_noFiles).MatchString
	is_buildFailed := regexp.MustCompile(gt_buildFailed).MatchString
	is_exit := regexp.MustCompile("^exit status -?\\d+").MatchString

	suites := []*Suite{}
	var curTest *Test
	var curSuite *Suite
	var out []string
	suiteStack := SuiteStack{}
	// Handles a test that ended with a panic.
	handlePanic := func() {
		curTest.Failed = true
		curTest.Skipped = false
		curTest.Time = "N/A"
		curSuite.Tests = append(curSuite.Tests, curTest)
		curTest = nil
	}

	// Appends output to the last test.
	appendError := func() error {
		if len(out) > 0 && curSuite != nil && len(curSuite.Tests) > 0 {
			message := strings.Join(out, "\n")
			if curSuite.Tests[len(curSuite.Tests)-1].Message == "" {
				curSuite.Tests[len(curSuite.Tests)-1].Message = message
			} else {
				curSuite.Tests[len(curSuite.Tests)-1].Message += "\n" + message
			}
		}
		out = []string{}
		return nil
	}

	scanner := NewLineScanner(rd)
	for scanner.Scan() {
		line := scanner.Text()

		// TODO: Only outside a suite/test, report as empty suite?
		if is_nofiles(line) {
			continue
		}

		if is_buildFailed(line) {
			return nil, fmt.Errorf("%d: package build failed: %s", scanner.Line(), line)
		}

		if curSuite == nil {
			curSuite = &Suite{}
		}

		tokens := find_start(line)
		if tokens != nil {
			if curTest != nil {
				// This occurs when the last test ended with a panic.
				if suiteStack.count == 0 {
					suiteStack.Push(curSuite)
					curSuite = &Suite{Name: curTest.Name}
				} else {
					handlePanic()
				}
			}
			if e := appendError(); e != nil {
				return nil, e
			}
			curTest = &Test{
				Name: tokens[1],
			}
			continue
		}

		tokens = find_end(line)
		if tokens != nil {
			if curTest == nil {
				if suiteStack.count > 0 {
					prevSuite := suiteStack.Pop()
					suites = append(suites, curSuite)
					curSuite = prevSuite
					continue
				} else {
					return nil, fmt.Errorf("%d: orphan end test", scanner.Line())
				}
			}
			if tokens[2] != curTest.Name {
				err := fmt.Errorf("%d: name mismatch (try disabling parallel mode)", scanner.Line())
				return nil, err
			}
			curTest.Failed = (tokens[1] == "FAIL") || (failOnRace && hasDatarace(out))
			curTest.Skipped = (tokens[1] == "SKIP")
			curTest.Passed = (tokens[1] == "PASS")
			curTest.Time = tokens[3]
			curTest.Message = strings.Join(out, "\n")
			curSuite.Tests = append(curSuite.Tests, curTest)
			curTest = nil
			out = []string{}
			continue
		}

		tokens = find_suite(line)
		if tokens != nil {
			if curTest != nil {
				// This occurs when the last test ended with a panic.
				handlePanic()
			}
			if e := appendError(); e != nil {
				return nil, e
			}
			curSuite.Name = tokens[2]
			curSuite.Time = tokens[3]
			suites = append(suites, curSuite)
			curSuite = nil
			continue
		}

		if is_exit(line) || (line == "FAIL") || (line == "PASS") {
			continue
		}

		out = append(out, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return suites, nil
}
