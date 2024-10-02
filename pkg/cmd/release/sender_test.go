// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"flag"
	"fmt"
	htmltemplate "html/template"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update .golden files")

// hookGlobal sets `*ptr = val` and returns a closure for restoring `*ptr` to
// its original value. A runtime panic will occur if `val` is not assignable to
// `*ptr`.
func hookGlobal(ptr, val interface{}) func() {
	global := reflect.ValueOf(ptr).Elem()
	orig := reflect.New(global.Type()).Elem()
	orig.Set(global)
	global.Set(reflect.ValueOf(val))
	return func() { global.Set(orig) }
}

func TestPickSHA(t *testing.T) {
	var expectedMessage *message
	defer hookGlobal(
		&sendmail,
		func(content *message, smtpOpts sendOpts) error {
			expectedMessage = content
			return nil
		})()

	args := messageDataPickSHA{
		Version:          "v21.1.13",
		SHA:              "0fd6eead6c6eb7b2529deb39197cc3c95e93ded8",
		TrackingIssue:    "REL-111",
		TrackingIssueURL: htmltemplate.URL("https://cockroachlabs.atlassian.net/browse/REL-111"),
		DiffURL:          "https://github.com/cockroachdb/cockroach/compare/v21.1.13...0fd6eead6c6eb7b2529deb39197cc3c95e93ded8",
	}
	require.NoError(t, sendMailPickSHA(
		args, sendOpts{templatesDir: "templates"},
	))
	verifyMessageValues(t, expectedMessage, templatePrefixPickSHA)
}

func TestCancelReleaseSeries(t *testing.T) {
	var expectedMessage *message
	defer hookGlobal(
		&sendmail,
		func(content *message, smtpOpts sendOpts) error {
			expectedMessage = content
			return nil
		})()

	args := messageDataCancelReleaseDate{
		Version:         "v21.1.13",
		ReleaseSeries:   "21.1",
		ReleaseDate:     "Feb 15, 2022",
		NextReleaseDate: "Mar 3, 2022",
	}
	require.NoError(t, sendMailCancelReleaseDate(
		args, sendOpts{templatesDir: "templates"},
	))
	verifyMessageValues(t, expectedMessage, templatePrefixPostBlockersPastPrep)
}

func TestPostBlockers(t *testing.T) {
	prepDate := time.Date(2022, 4, 2, 0, 0, 0, 0, time.UTC)
	releaseDate := time.Date(2022, 4, 11, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		testCase            string
		version             string
		daysBeforePrep      daysBeforePrep
		blockersPerProject  []int
		goldenFilePrefix    string
		expectedContains    []string
		expectedNotContains []string
	}{
		{
			testCase:           "alpha: zero-blockers",
			version:            "v19.1.0-alpha.3",
			daysBeforePrep:     daysBeforePrepManyDays,
			blockersPerProject: []int{},
			goldenFilePrefix:   templatePrefixPostBlockersAlpha + ".zero-blockers",
			expectedContains: []string{
				"We are clear to proceed with preparation and qualification",
			},
		},
		{
			testCase:           "alpha: 1-blocker",
			version:            "v19.1.0-alpha.3",
			daysBeforePrep:     daysBeforePrepManyDays,
			blockersPerProject: []int{1},
			goldenFilePrefix:   templatePrefixPostBlockersAlpha + ".1-blocker",
			expectedNotContains: []string{
				"which must be resolved before a candidate is chosen",
			},
		},
		{
			testCase:           "alpha: many-blockers",
			version:            "v19.1.0-alpha.3",
			daysBeforePrep:     daysBeforePrepManyDays,
			blockersPerProject: []int{4, 2, 3},
			goldenFilePrefix:   templatePrefixPostBlockersAlpha + ".many-blockers",
			expectedNotContains: []string{
				"which must be resolved before a candidate is chosen",
				"reminder to merge any outstanding backports",
				"reviewed at Monday's triage meeting",
			},
		},
		{
			testCase:           "non-alpha: zero-blockers. stable/production: refer to backboard",
			version:            "v19.1.11",
			daysBeforePrep:     daysBeforePrepManyDays,
			blockersPerProject: []int{},
			goldenFilePrefix:   templatePrefixPostBlockers + ".zero-blockers",
			expectedContains: []string{
				"We are clear to proceed with preparation and qualification",
				"backboard.crdb.dev",
			},
			expectedNotContains: []string{
				"reviewed at Monday's triage meeting",
			},
		},
		{
			testCase:           "non-alpha: 1-blocker. beta/rc's are reviewed in triage meeting",
			version:            "v19.1.0-beta.2",
			daysBeforePrep:     daysBeforePrepManyDays,
			blockersPerProject: []int{1},
			goldenFilePrefix:   templatePrefixPostBlockers + ".1-blocker",
			expectedContains: []string{
				"which must be resolved before a candidate is chosen",
				"reviewed at Monday's triage meeting",
			},
			expectedNotContains: []string{
				"backboard.crdb.dev",
			},
		},
		{
			testCase:           "non-alpha: many-blockers. beta/rc's are reviewed in triage meeting",
			version:            "v19.1.0-rc.3",
			daysBeforePrep:     daysBeforePrepManyDays,
			blockersPerProject: []int{4, 2, 3},
			goldenFilePrefix:   templatePrefixPostBlockers + ".many-blockers",
			expectedContains: []string{
				"which must be resolved before a candidate is chosen",
				"reviewed at Monday's triage meeting",
			},
			expectedNotContains: []string{
				"backboard.crdb.dev",
			},
		},
		{
			testCase:           "stable/production, day before prep date",
			version:            "v19.1.11",
			daysBeforePrep:     daysBeforePrepDayBefore,
			blockersPerProject: []int{4, 2, 3},
			goldenFilePrefix:   templatePrefixPostBlockers + ".stable.day-before-prep-date",
			expectedContains: []string{
				"tonight's nightly build",
				"Friendly reminder to merge any outstanding backports",
				"which must be resolved before a candidate is chosen",
			},
		},
		{
			testCase:           "stable/production, day of prep date",
			version:            "v19.1.11",
			daysBeforePrep:     daysBeforePrepDayOf,
			blockersPerProject: []int{4, 2, 3},
			goldenFilePrefix:   templatePrefixPostBlockers + ".stable.day-of-prep-date",
			expectedContains: []string{
				"will be cancelled tomorrow",
				"if the following release blockers are not resolved before tonight's nightly build",
				"which must be resolved before a candidate is chosen",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testCase, func(t *testing.T) {
			var expectedMessage *message
			defer hookGlobal(
				&sendmail,
				func(content *message, smtpOpts sendOpts) error {
					expectedMessage = content
					return nil
				})()

			// generate test data for blockerList
			totalBlockers := 0
			var blockerList []ProjectBlocker
			for i, numBlockers := range test.blockersPerProject {
				blockerList = append(blockerList, ProjectBlocker{
					ProjectName: fmt.Sprintf("Project %d", i+1),
					NumBlockers: numBlockers,
				})
				totalBlockers += numBlockers
			}
			args := messageDataPostBlockers{
				Version:           test.version,
				DaysBeforePrep:    test.daysBeforePrep,
				DayBeforePrepDate: prepDate.AddDate(0, 0, -1).Format(dateFormatEmail),
				PrepDate:          prepDate.Format(dateFormatEmail),
				ReleaseDate:       releaseDate.Format(dateFormatEmail),
				TotalBlockers:     totalBlockers,
				BlockersURL:       "go.crdb.dev/blockers",
				ReleaseBranch:     "master",
				BlockerList:       blockerList,
			}
			require.NoError(t, sendMailPostBlockers(
				args, sendOpts{templatesDir: "templates"},
			))
			verifyMessageValues(t, expectedMessage, test.goldenFilePrefix)
			for _, expectedContains := range test.expectedContains {
				require.Contains(t, expectedMessage.HTMLBody, expectedContains)
				require.Contains(t, expectedMessage.TextBody, expectedContains)
			}
			for _, expectedNotContains := range test.expectedNotContains {
				require.NotContains(t, expectedMessage.HTMLBody, expectedNotContains)
				require.NotContains(t, expectedMessage.TextBody, expectedNotContains)
			}
		})
	}
}

func verifyMessageValue(t *testing.T, expected string, goldenFileName string) {
	templates := mockMailTemplates()

	if *update {
		templates.CreateGolden(t, expected, goldenFileName)
	}

	goldenFile := templates.GetGolden(t, goldenFileName)

	require.Equal(t, goldenFile, expected)
}

func verifyMessageValues(t *testing.T, msg *message, goldenFilePrefix string) {
	goldenTextFileName := goldenFilePrefix + textBodyGoldenFileSuffix
	goldenHTMLFileName := goldenFilePrefix + htmlBodyGoldenFileSuffix
	goldenSubjectFileName := goldenFilePrefix + subjectGoldenFileSuffix

	verifyMessageValue(t, msg.TextBody, goldenTextFileName)
	verifyMessageValue(t, msg.HTMLBody, goldenHTMLFileName)
	verifyMessageValue(t, msg.Subject, goldenSubjectFileName)
}

type mailTemplates struct {
}

func mockMailTemplates() *mailTemplates {
	return &mailTemplates{}
}

const (
	subjectGoldenFileSuffix  = ".subject.golden"
	textBodyGoldenFileSuffix = ".txt.golden"
	htmlBodyGoldenFileSuffix = ".html.golden"
	testDataDirectory        = "testdata"
)

/*

Golden Files

Golden files are files containing expected output (like Snapshots in Jest). We use them here to
compare expected output of email templates to generated output. For every email that is sent there
are three associated files; an HTML file, a Plain Text file, and a file containing the Subject.
Testing these files can be done with the convinience method `verifyMessageValues`. For Example,

  func blahblahEmailTest (t *testing.T) {
    ...

    messageValues := MessageValues{
      Subject:  "blah blah subject",
      TextBody: "blah blah text body",
      HtmlBody: "<main>blah blah html stuff</main>",
    }
    goldenFilePrefix := "very_cool_email"

    verifyMessageValues(t, MessageValues, goldenFilePrefix)
  }

To generate golden files you may run go test with the update flag

    go test -v ./pkg/cmd/release -update

*/

// CreateGolden generates Jest-like snapshots used to verify Mailer message formatting.
func (mt *mailTemplates) CreateGolden(t *testing.T, actualValue string, filename string) {
	err := os.WriteFile(filepath.Join(testDataDirectory, filename), []byte(actualValue), 0644)
	require.NoError(t, err)
}

// GetGolden verifies Mailer-generated messages against golden files generated by CreateGolden.
func (mt *mailTemplates) GetGolden(t *testing.T, filename string) string {
	file, err := os.ReadFile(filepath.Join(testDataDirectory, filename))
	require.NoError(t, err)
	return string(file)
}
