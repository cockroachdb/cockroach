// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type githubTestClient struct {
	issues []githubIssue
	events []githubEvent
}

var _ githubClient = &githubTestClient{}

func newTestClient(issues []githubIssue) *githubTestClient {
	return &githubTestClient{
		issues: issues,
	}
}

func (c *githubTestClient) openIssues(_ []string) ([]githubIssue, error) {
	return c.issues, nil
}

func (c *githubTestClient) issueEvents(_ int) ([]githubEvent, error) {
	return c.events, nil
}

func (c *githubTestClient) addEvents(t *testing.T, events ...githubEvent) {
	length := len(c.events)
	for i, e := range events {
		// first event will be 2006-01-01, second event 2006-01-02, etc
		e.CreatedAt = testCreatedAtDay(t, length+i+1)
		c.events = append(c.events, e)
	}
}

// testCreatedAtDay returns time.Time object for Jan {day}, 2006.
func testCreatedAtDay(t *testing.T, day int) time.Time {
	date, err := time.Parse(time.RFC3339, fmt.Sprintf("2006-01-%02dT00:00:00Z", day))
	require.NoError(t, err)
	return date
}

func TestFetchOpenBlockers(t *testing.T) {
	tests := []struct {
		descr            string
		openIssues       []githubIssue
		expectedBlockers *openBlockers
	}{
		{
			descr:            "no open issues",
			openIssues:       []githubIssue{},
			expectedBlockers: &openBlockers{},
		},
		{
			descr: "one issue, has project",
			openIssues: []githubIssue{
				{1, "AAAA"},
			},
			expectedBlockers: &openBlockers{
				TotalBlockers: 1,
				BlockerList: []ProjectBlocker{
					{ProjectName: "AAAA", NumBlockers: 1},
				},
			},
		},
		{
			descr: "two issues, no project",
			openIssues: []githubIssue{
				{1, ""},
				{2, ""},
			},
			expectedBlockers: &openBlockers{
				TotalBlockers: 2,
				BlockerList: []ProjectBlocker{
					{ProjectName: NoProjectName, NumBlockers: 2},
				},
			},
		},
		{
			descr: "multiple issues, with/without projects, 'no project' should still be last",
			openIssues: []githubIssue{
				{1, "BBBB"},
				{2, "AAAA"},
				{3, ""},
				{4, "CCCC"},
				{5, ""},
				{6, "AAAA"},
				{7, "AAAA"},
			},
			expectedBlockers: &openBlockers{
				TotalBlockers: 7,
				BlockerList: []ProjectBlocker{
					{ProjectName: "AAAA", NumBlockers: 3},
					{ProjectName: "BBBB", NumBlockers: 1},
					{ProjectName: "CCCC", NumBlockers: 1},
					{ProjectName: NoProjectName, NumBlockers: 2},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.descr, func(t *testing.T) {
			testClient := newTestClient(test.openIssues)
			blockers, err := fetchOpenBlockers(testClient, "21.2")
			require.NoError(t, err)
			require.EqualValues(t, test.expectedBlockers, blockers)
		})
	}
}

func TestMostRecentProjectName(t *testing.T) {
	testIssueNum := 12345
	testClient := newTestClient(nil /* issues */)

	// these steps build upon each other, i.e. the events from the previous steps
	// stay in the client's list of events.
	steps := []struct {
		eventsToAdd         []githubEvent
		expectedProjectName string
	}{
		{
			eventsToAdd:         []githubEvent{},
			expectedProjectName: "",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventAddedProject, ProjectName: "AAA"},
			},
			expectedProjectName: "AAA",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventRemovedProject, ProjectName: "AAA"},
			},
			expectedProjectName: "",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventAddedProject, ProjectName: "AAA"},
				{Event: eventRemovedProject, ProjectName: "AAA"},
			},
			expectedProjectName: "",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventAddedProject, ProjectName: "AAA"},
			},
			expectedProjectName: "AAA",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventAddedProject, ProjectName: "BBB"},
				{Event: eventAddedProject, ProjectName: "CCC"},
				{Event: eventAddedProject, ProjectName: "DDD"},
			},
			expectedProjectName: "DDD",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventRemovedProject, ProjectName: "CCC"},
			},
			expectedProjectName: "DDD",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventRemovedProject, ProjectName: "DDD"},
			},
			expectedProjectName: "BBB",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventRemovedProject, ProjectName: "AAA"},
			},
			expectedProjectName: "BBB",
		},
		{
			eventsToAdd: []githubEvent{
				{Event: eventRemovedProject, ProjectName: "BBB"},
			},
			expectedProjectName: "",
		},
	}
	for _, step := range steps {
		testClient.addEvents(t, step.eventsToAdd...)
		projectName, err := mostRecentProjectName(testClient, testIssueNum)
		require.NoError(t, err)
		require.Equal(t, step.expectedProjectName, projectName)
	}
}

func TestSortGithubEvents(t *testing.T) {
	events := githubEvents{
		{CreatedAt: testCreatedAtDay(t, 3), Event: eventAddedProject, ProjectName: "333"},
		{CreatedAt: testCreatedAtDay(t, 6), Event: eventAddedProject, ProjectName: "666"},
		{CreatedAt: testCreatedAtDay(t, 4), Event: eventAddedProject, ProjectName: "444"},
		{CreatedAt: testCreatedAtDay(t, 5), Event: eventAddedProject, ProjectName: "555"},
		{CreatedAt: testCreatedAtDay(t, 1), Event: eventAddedProject, ProjectName: "111"},
		{CreatedAt: testCreatedAtDay(t, 2), Event: eventAddedProject, ProjectName: "222"},
	}
	sort.Sort(events)
	require.EqualValues(t, githubEvents{
		{CreatedAt: testCreatedAtDay(t, 1), Event: eventAddedProject, ProjectName: "111"},
		{CreatedAt: testCreatedAtDay(t, 2), Event: eventAddedProject, ProjectName: "222"},
		{CreatedAt: testCreatedAtDay(t, 3), Event: eventAddedProject, ProjectName: "333"},
		{CreatedAt: testCreatedAtDay(t, 4), Event: eventAddedProject, ProjectName: "444"},
		{CreatedAt: testCreatedAtDay(t, 5), Event: eventAddedProject, ProjectName: "555"},
		{CreatedAt: testCreatedAtDay(t, 6), Event: eventAddedProject, ProjectName: "666"},
	}, events)
}

func TestSortProjectBlockers(t *testing.T) {
	// list1: all have non-project project name; not alphabetized
	list1 := projectBlockers{
		{"BBBB", 1111},
		{"CCCC", 1111},
		{"AAAA", 1111},
	}
	var listCopy projectBlockers
	copy(listCopy, list1)

	sort.Sort(list1)
	// list1: verify that list is alphabetized by project name
	require.NotEqualValues(t, listCopy, list1)
	require.EqualValues(t, projectBlockers{
		{"AAAA", 1111},
		{"BBBB", 1111},
		{"CCCC", 1111},
	}, list1)

	// list2: some project names, one with "No Project"; not alphabetized
	list2 := projectBlockers{
		{NoProjectName, 1111},
		{"CCCC", 1111},
		{"AAAA", 1111},
	}
	copy(listCopy, list2)

	// list2: verify that list is alphabetized by project name, except for
	// "No Project", which should be last
	sort.Sort(list2)
	require.NotEqualValues(t, listCopy, list2)
	require.EqualValues(t, projectBlockers{
		{"AAAA", 1111},
		{"CCCC", 1111},
		{NoProjectName, 1111},
	}, list2)
}
