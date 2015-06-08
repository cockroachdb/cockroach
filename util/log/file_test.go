// Go support for leveled logs, analogous to https://code.google.com/p/google-clog/
//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Author: Bram Gruneir (bram@cockroachlabs.com)

package log

import (
	"testing"
	"time"
)

// TestLogFilenameParsing ensures that logName and parseLogFilename work as
// advertised.
func TestLogFilenameParsing(t *testing.T) {
	testCases := []struct {
		Severity Severity
		Time     time.Time
	}{
		{InfoLog, time.Now()},
		{WarningLog, time.Now().AddDate(-10, 0, 0)},
		{ErrorLog, time.Now().AddDate(0, 0, -1)},
	}

	for i, testCase := range testCases {
		filename, _ := logName(testCase.Severity, testCase.Time)
		details, err := parseLogFilename(filename)
		if err != nil {
			t.Fatal(err)
		}
		if details.Severity != testCase.Severity {
			t.Errorf("%d: Severities do not match, expected:%s - actual:%s", i, testCase.Severity.Name(), details.Severity.Name())
		}
		if details.Time.Format(time.RFC3339) != testCase.Time.Format(time.RFC3339) {
			t.Errorf("%d: Times do not match, expected:%v - actual:%v", i, testCase.Time.Format(time.RFC3339), details.Time.Format(time.RFC3339))
		}
	}
}

// TestSelectFiles checks that selectFiles correctly filters and orders
// filesInfos.
func TestSelectFiles(t *testing.T) {
	testFiles := []FileInfo{}
	year2000 := time.Date(2000, time.January, 1, 1, 0, 0, 0, time.UTC)
	year2050 := time.Date(2050, time.January, 1, 1, 0, 0, 0, time.UTC)
	year2200 := time.Date(2200, time.January, 1, 1, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		sev := Severity(i % 3)
		fileTime := year2000.AddDate(i, 0, 0)
		name, _ := logName(sev, fileTime)
		testfile := FileInfo{
			Name: name,
			Details: FileDetails{
				Severity: sev,
				Time:     fileTime,
			},
		}
		testFiles = append(testFiles, testfile)
	}

	testCases := []struct {
		Severity      Severity
		EndTimestamp  int64
		ExpectedCount int
	}{
		{InfoLog, year2200.UnixNano(), 34},
		{WarningLog, year2200.UnixNano(), 33},
		{ErrorLog, year2200.UnixNano(), 33},
		{InfoLog, year2050.UnixNano(), 17},
		{WarningLog, year2050.UnixNano(), 17},
		{ErrorLog, year2050.UnixNano(), 17},
		{InfoLog, year2000.UnixNano(), 1},
		{WarningLog, year2000.UnixNano(), 0},
		{ErrorLog, year2000.UnixNano(), 0},
	}

	for i, testCase := range testCases {
		actualFiles := selectFiles(testFiles, testCase.Severity, testCase.EndTimestamp)
		previousTimestamp := year2200.UnixNano()
		if len(actualFiles) != testCase.ExpectedCount {
			t.Fatalf("%d: expected %d files, actual %d", i, testCase.ExpectedCount, len(actualFiles))
		}
		for _, file := range actualFiles {
			if file.Details.Time.UnixNano() > previousTimestamp {
				t.Fatalf("%d: returned files are not in the correct order", i)
			}
			if file.Details.Severity != testCase.Severity {
				t.Fatalf("%d: did not filter by severity", i)
			}
			if file.Details.Time.UnixNano() > testCase.EndTimestamp {
				t.Fatalf("%d: did not filter by endTime", i)
			}
			previousTimestamp = file.Details.Time.UnixNano()
		}
	}
}
