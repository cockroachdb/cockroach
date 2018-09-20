// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// This test runs hibernate-core's full test suite against an single cockroach
// node.

func registerHibernate(r *registry) {
	runHibernate := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, c.All())

		t.Status("cloning hibernate and installing prerequisites")
		c.Run(ctx, node, `rm -rf /mnt/data1/hibernate`)
		c.GitClone(ctx, "https://github.com/hibernate/hibernate-orm.git", "/mnt/data1/hibernate", "5.3.6", node)
		c.Run(ctx, node, `sudo apt-get -q update`)
		c.Run(ctx, node, `yes | sudo apt-get -q install default-jre openjdk-8-jdk-headless gradle`)

		// In order to get Hibernate's test suite to connect to cockroach, we have
		// to create a dbBundle as it not possible to specify the individual
		// properties. So here we just steamroll the file with our own config.
		c.Run(ctx, node, `echo "ext {
					db = project.hasProperty('db') ? project.getProperty('db') : 'h2'
					dbBundle = [
									cockroach : [
													'db.dialect' : 'org.hibernate.dialect.PostgreSQL95Dialect',
													'jdbc.driver': 'org.postgresql.Driver',
													'jdbc.user'  : 'root',
													'jdbc.pass'  : '',
													'jdbc.url'   : 'jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable'
									],
					]
		}" > /mnt/data1/hibernate/gradle/databases.gradle`)

		t.Status("running hibernate test suite, will take at least 3 hours")
		// When testing, it is helpful to run only a subset of the tests. To do so
		// add "--tests org.hibernate.test.annotations.lob.*" to the end of the
		// test run command.
		// Note that this will take upwards of 3 hours.
		c.RunE(ctx, node, `cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=cockroach`)

		t.Status("collecting the test results")
		// Copy all of the test results to the cockroach logs directory to be
		// copied to the artifacts.

		// Copy the html report for the test.
		c.Run(ctx, node, `cp /mnt/data1/hibernate/hibernate-core/target/reports/tests/test ~/logs/report -a`)

		// Copy the individual test result files.
		c.Run(ctx, node, `cp /mnt/data1/hibernate/hibernate-core/target/test-results/test ~/logs/report/results -a`)

		// Convert the blacklist into a map.
		expectedFailures := make(map[string]struct{})
		for _, failure := range hibernateBlackList {
			expectedFailures[failure] = struct{}{}
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/hibernate/hibernate-core/target/test-results/test
		nodeString := c.makeNodes(node)
		cmd := exec.CommandContext(ctx, roachprod, "run", nodeString, `ls /mnt/data1/hibernate/hibernate-core/target/test-results/test/*.xml`)
		output, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}

		var passExpected []string
		var passUnexpected []string
		var failExpected []string
		var failUnexpected []string
		files := strings.Split(string(output), "\n")
		for i, file := range files {
			file = strings.TrimSpace(file)
			if len(file) == 0 {
				continue
			}
			c.l.Printf("Parsing %d of %d: %s\n", i, len(files), file)
			fileCmd := exec.CommandContext(ctx, roachprod, "run", nodeString, `cat `+file)
			fileOutput, err := fileCmd.Output()
			tests, passed, err := extractFailureFromHibernateXML(fileOutput)
			if err != nil {
				t.Fatal(err)
			}
			for i, test := range tests {
				_, expectedFailure := expectedFailures[test]
				pass := passed[i]
				switch {
				case pass && !expectedFailure:
					passExpected = append(passExpected, test)
				case pass && expectedFailure:
					passUnexpected = append(passUnexpected, test)
				case !pass && expectedFailure:
					failExpected = append(failExpected, test)
				case !pass && !expectedFailure:
					failUnexpected = append(failUnexpected, test)
				}
				if expectedFailure {
					delete(expectedFailures, test)
				}
			}
		}
		var notRunExpectedFail []string
		for test := range expectedFailures {
			notRunExpectedFail = append(notRunExpectedFail, test)
		}

		t.Status("writing results back to the cluster's logs")
		writeResults := func(tests []string, name string, asGoArray bool) {
			sort.Strings(tests)
			file, err := ioutil.TempFile("", "name")
			if err != nil {
				t.Fatal(err)
			}
			if asGoArray {
				fmt.Fprintf(file, "var %s = []string{\n", name)
			}
			for _, test := range tests {
				if asGoArray {
					fmt.Fprintf(file, "\"%s\",\n", test)
				} else {
					fmt.Fprintf(file, "%s\n", test)
				}
			}
			if asGoArray {
				fmt.Fprintf(file, "}\n")
			}
			fmt.Fprintf(file, "\n")
			c.Put(ctx, file.Name(), fmt.Sprintf("~/logs/%s.txt", name))
			if err := os.Remove(file.Name()); err != nil {
				t.Fatal(err)
			}
		}

		writeResults(passExpected, "passExpected", false /* asGoArray */)
		writeResults(passUnexpected, "passUnexpected", false /* asGoArray */)
		writeResults(failExpected, "failExpected", false /* asGoArray */)
		writeResults(failUnexpected, "failUnexpected", false /* asGoArray */)
		writeResults(notRunExpectedFail, "notRunExpectedFail", false /* asGoArray */)

		// Create a new hibernate_blacklist so we can easily update this test.
		currentFailures := append(append([]string{}, failExpected...), failUnexpected...)
		writeResults(currentFailures, "hibernateBlackList", true /* asGoArray */)

		c.l.Printf("------------------------\n")
		c.l.Printf("%d Total Test Run\n", len(passExpected)+len(passUnexpected)+len(failExpected)+len(failUnexpected))
		c.l.Printf("%d tests passed\n", len(passUnexpected)+len(passExpected))
		c.l.Printf("%d tests failed\n", len(failUnexpected)+len(failExpected))
		c.l.Printf("%d tests passed unexpectedly\n", len(passUnexpected))
		c.l.Printf("%d tests failed unexpectedly\n", len(failUnexpected))
		c.l.Printf("For a full summary, look at artifacts/hibernate/logs/logs/report/index.html\n")
		c.l.Printf("------------------------\n")

		if len(failUnexpected) > 0 || len(passUnexpected) > 0 || len(notRunExpectedFail) > 0 {
			t.Fatalf("\n%d tests failed unexpectedly\n"+
				"%d tests passed unexpectedly\n"+
				"%d tests were not run that were expected to fail\n"+
				"See artifacts/hibernate/log/logs/report for more details",
				len(failUnexpected), len(passUnexpected), len(notRunExpectedFail))
		}
	}

	r.Add(testSpec{
		Name:   "hibernate",
		Nodes:  nodes(1),
		Stable: false,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runHibernate(ctx, t, c)
		},
	})
}

func extractFailureFromHibernateXML(contents []byte) ([]string, []bool, error) {
	type Failure struct {
		Message     string `xml:"message,attr"`
		FailureType string `xml:"type,attr"`
	}
	type TestCase struct {
		Name      string  `xml:"name,attr"`
		ClassName string  `xml:"classname,attr"`
		Failure   Failure `xml:"failure,omitempty"`
	}
	type TestSuite struct {
		XMLName xml.Name `xml:"testsuite"`
		Name    string   `xml:"name,attr"`
		Tests   int      `xml:"tests,attr"`

		TestCases []TestCase `xml:"testcase"`
	}
	var testSuite TestSuite
	if err := xml.Unmarshal([]byte(contents), &testSuite); err != nil {
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
