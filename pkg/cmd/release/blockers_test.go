package main

import (
  "github.com/stretchr/testify/require"
  "sort"
  "testing"
)

type githubTestClient struct {
  issues []githubIssue
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

func TestFetchOpenBlockers(t *testing.T) {
  tests := []struct {
    descr            string
    openIssues       []githubIssue
    expectedBlockers []ProjectBlocker
  }{
    {
      descr:            "no open issues",
      openIssues:       []githubIssue{},
      expectedBlockers: []ProjectBlocker{},
    },
    {
      descr: "one issue, has project",
      openIssues: []githubIssue{
        {1, "AAAA"},
      },
      expectedBlockers: []ProjectBlocker{
        {ProjectName: "AAAA", NumBlockers: 1},
      },
    },
    {
      descr: "two issues, no project",
      openIssues: []githubIssue{
        {1, ""},
        {2, ""},
      },
      expectedBlockers: []ProjectBlocker{
        {ProjectName: NoProjectName, NumBlockers: 2},
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
      expectedBlockers: []ProjectBlocker{
        {ProjectName: "AAAA", NumBlockers: 3},
        {ProjectName: "BBBB", NumBlockers: 1},
        {ProjectName: "CCCC", NumBlockers: 1},
        {ProjectName: NoProjectName, NumBlockers: 2},
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
