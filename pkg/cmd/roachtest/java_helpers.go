// Copyright 2019 The Cockroach Authors.
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
	"encoding/xml"
	"fmt"
)

func extractFailureFromJUnitXML(contents []byte) ([]string, []bool, error) {
	type Failure struct {
		Message string `xml:"message,attr"`
	}
	type TestCase struct {
		Name      string  `xml:"name,attr"`
		ClassName string  `xml:"classname,attr"`
		Failure   Failure `xml:"failure,omitempty"`
	}
	type TestSuite struct {
		XMLName   xml.Name   `xml:"testsuite"`
		TestCases []TestCase `xml:"testcase"`
	}

	var testSuite TestSuite
	_ = testSuite.XMLName

	if err := xml.Unmarshal(contents, &testSuite); err != nil {
		return nil, nil, err
	}

	var tests []string
	var passed []bool
	for _, testCase := range testSuite.TestCases {
		tests = append(tests, fmt.Sprintf("%s.%s", testCase.ClassName, testCase.Name))
		passed = append(passed, len(testCase.Failure.Message) == 0)
	}

	return tests, passed, nil
}
