// Copyright 2013 Google Inc. All Rights Reserved.
// Copyright 2017 The Cockroach Authors.
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

// This code originated in the github.com/golang/glog package.

package log

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestLogFilenameParsing ensures that logName and parseLogFilename work as
// advertised.
func TestLogFilenameParsing(t *testing.T) {
	testCases := []time.Time{
		timeutil.Now(),
		timeutil.Now().AddDate(-10, 0, 0),
		timeutil.Now().AddDate(0, 0, -1),
	}

	for i, testCase := range testCases {
		filename, _ := logName(program, testCase)
		details, err := parseLogFilename(filename)
		if err != nil {
			t.Fatal(err)
		}
		if a, e := timeutil.Unix(0, details.Time).Format(time.RFC3339), testCase.Format(time.RFC3339); a != e {
			t.Errorf("%d: Times do not match, expected:%s - actual:%s", i, e, a)
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
		fileTime := year2000.AddDate(i, 0, 0)
		name, _ := logName(program, fileTime)
		testfile := FileInfo{
			Name: name,
			Details: FileDetails{
				Time: fileTime.UnixNano(),
			},
		}
		testFiles = append(testFiles, testfile)
	}

	testCases := []struct {
		EndTimestamp  int64
		ExpectedCount int
	}{
		{year2200.UnixNano(), 100},
		{year2050.UnixNano(), 51},
		{year2000.UnixNano(), 1},
	}

	for i, testCase := range testCases {
		actualFiles := selectFiles(testFiles, testCase.EndTimestamp)
		previousTimestamp := year2200.UnixNano()
		if len(actualFiles) != testCase.ExpectedCount {
			t.Errorf("%d: expected %d files, actual %d", i, testCase.ExpectedCount, len(actualFiles))
		}
		for _, file := range actualFiles {
			if file.Details.Time > previousTimestamp {
				t.Errorf("%d: returned files are not in the correct order", i)
			}
			if file.Details.Time > testCase.EndTimestamp {
				t.Errorf("%d: did not filter by endTime", i)
			}
			previousTimestamp = file.Details.Time
		}
	}
}
