// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file contains assorted utilities for working with Bazel internals.

package util

import (
	"cmp"
	"encoding/xml"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Below are data structures representing the `test.xml` schema.
// Ref: https://github.com/bazelbuild/rules_go/blob/master/go/tools/bzltestutil/xml.go

// TestSuites represents the contents of a single test.xml file (<testsuites>).
type TestSuites struct {
	XMLName xml.Name    `xml:"testsuites"`
	Suites  []testSuite `xml:"testsuite"`
}

type testSuite struct {
	XMLName   xml.Name    `xml:"testsuite"`
	TestCases []*testCase `xml:"testcase"`
	Attrs     []xml.Attr  `xml:",any,attr"`
	Name      string      `xml:"name,attr"`
}

type testCase struct {
	XMLName xml.Name `xml:"testcase"`
	// Note that we deliberately exclude the `classname` attribute. It never
	// contains useful information (always just the name of the package --
	// this isn't Java so there isn't a classname) and excluding it causes
	// the TeamCity UI to display the same data in a slightly more coherent
	// and usable way.
	Name    string      `xml:"name,attr"`
	Time    string      `xml:"time,attr"`
	Failure *XMLMessage `xml:"failure,omitempty"`
	Error   *XMLMessage `xml:"error,omitempty"`
	Skipped *XMLMessage `xml:"skipped,omitempty"`
}

// XMLMessage is a catch-all structure containing details about a test
// failure.
type XMLMessage struct {
	Message  string     `xml:"message,attr"`
	Attrs    []xml.Attr `xml:",any,attr"`
	Contents string     `xml:",chardata"`
}

// OutputOfBinaryRule returns the path of the binary produced by the
// given build target, relative to bazel-bin. That is,
//
//	filepath.Join(bazelBin, OutputOfBinaryRule(target, isWindows)) is the absolute
//
// path to the build binary for the target.
func OutputOfBinaryRule(target string, isWindows bool) string {
	colon := strings.Index(target, ":")
	var bin string
	if colon >= 0 {
		bin = target[colon+1:]
	} else {
		bin = target[strings.LastIndex(target, "/")+1:]
	}
	var head string
	if strings.HasPrefix(target, "@") {
		doubleSlash := strings.Index(target, "//")
		joinArgs := []string{"external", target[1:doubleSlash]}
		joinArgs = append(joinArgs, strings.Split(target[doubleSlash+2:colon], "/")...)
		head = filepath.Join(joinArgs...)
	} else if colon >= 0 {
		head = strings.TrimPrefix(target[:colon], "//")
	} else {
		head = strings.TrimPrefix(target, "//")
	}
	if isWindows {
		return filepath.Join(head, bin+"_", bin+".exe")
	}
	return filepath.Join(head, bin+"_", bin)
}

// OutputsOfGenrule lists the outputs of a genrule. The first argument
// is the name of the target (e.g. //docs/generated/sql), and the second
// should be the output of `bazel query --output=xml $TARGET`. The
// returned slice is the list of outputs, all of which are relative
// paths atop `bazel-bin` as in `OutputOfBinaryRule`.
func OutputsOfGenrule(target, xmlQueryOutput string) ([]string, error) {
	// XML parsing is a bit heavyweight here, and encoding/xml
	// refuses to parse the query output since it's XML 1.1 instead
	// of 1.0. Have fun with regexes instead.
	colon := strings.LastIndex(target, ":")
	if colon < 0 {
		colon = len(target)
	}
	regexStr := fmt.Sprintf("^<rule-output name=\"%s:(?P<Filename>.*)\"/>$", regexp.QuoteMeta(target[:colon]))
	re, err := regexp.Compile(regexStr)
	if err != nil {
		return nil, err
	}
	var ret []string
	for _, line := range strings.Split(xmlQueryOutput, "\n") {
		line = strings.TrimSpace(line)
		submatch := re.FindStringSubmatch(line)
		if submatch == nil {
			continue
		}
		relBinPath := filepath.Join(strings.TrimPrefix(target[:colon], "//"), submatch[1])
		ret = append(ret, relBinPath)
	}
	return ret, nil
}

// MungeTestXML parses and slightly munges the XML in the source file and writes
// it to the output file. TeamCity kind of knows how to interpret the schema,
// but the schema isn't *exactly* what it's expecting. By munging the XML's
// here we ensure that the TC test view is as useful as possible.
// Helper function meant to be used with maybeStageArtifact.
func MungeTestXML(srcContent []byte, outFile io.Writer) error {
	// Parse the XML into a TestSuites struct.
	suites := TestSuites{}
	err := xml.Unmarshal(srcContent, &suites)
	// Note that we return an error if parsing fails. This isn't
	// unexpected -- if we read the XML file before it's been
	// completely written to disk, that will happen. Returning the
	// error will cancel the write to disk, which is exactly what we
	// want.
	if err != nil {
		return err
	}
	// We only want the first test suite in the list of suites.
	return writeToFile(&suites.Suites[0], outFile)
}

// MergeTestXMLs merges the given list of test suites into a single test suite,
// then writes the serialized XML to the given outFile. The prefix is passed
// to xml.Unmarshal. Note that this function might modify the passed-in
// TestSuites in-place.
func MergeTestXMLs(suitesToMerge []TestSuites, outFile io.Writer) error {
	if len(suitesToMerge) == 0 {
		return fmt.Errorf("expected at least one test suite")
	}
	var resultSuites TestSuites
	resultSuites.Suites = append(resultSuites.Suites, testSuite{})
	resultSuite := &resultSuites.Suites[0]
	resultSuite.Name = suitesToMerge[0].Suites[0].Name
	resultSuite.Attrs = suitesToMerge[0].Suites[0].Attrs
	cases := make(map[string]*testCase)
	for _, suites := range suitesToMerge {
		for _, testCase := range suites.Suites[0].TestCases {
			oldCase, ok := cases[testCase.Name]
			if !ok {
				cases[testCase.Name] = testCase
				continue
			}
			if testCase.Failure != nil {
				if oldCase.Failure == nil {
					oldCase.Failure = testCase.Failure
				} else {
					oldCase.Failure.Contents = oldCase.Failure.Contents + "\n" + testCase.Failure.Contents
				}
			}
			if testCase.Error != nil {
				if oldCase.Error == nil {
					oldCase.Error = testCase.Error
				} else {
					oldCase.Error.Contents = oldCase.Error.Contents + "\n" + testCase.Error.Contents
				}
			}
		}
	}
	for _, testCase := range cases {
		resultSuite.TestCases = append(resultSuite.TestCases, testCase)
	}
	slices.SortFunc(resultSuite.TestCases, func(a, b *testCase) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return writeToFile(&resultSuites, outFile)
}

func writeToFile(suite interface{}, outFile io.Writer) error {
	bytes, err := xml.MarshalIndent(suite, "", "\t")
	if err != nil {
		return err
	}
	_, err = outFile.Write(bytes)
	if err != nil {
		return err
	}
	// Insert a newline just to make our lives a little easier.
	_, err = outFile.Write([]byte("\n"))
	return err
}
