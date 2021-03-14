// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindTeamsFromKeywords(t *testing.T) {
	testCases := []struct {
		body     string
		expected map[string][]string
	}{
		{
			`my rocks db is bad`,
			map[string][]string{
				"storage": {"rocks db"},
			},
		},
		{
			`my backup is failing! i executed this SQL statement to get it`,
			map[string][]string{
				"bulk-io":        {"backup"},
				"sql-experience": {"SQL statement"},
			},
		},
		// Ensure default templates don't cause anyone to get pinged.
		{
			`**Is your feature request related to a problem? Please describe.**
A clear and concise description of what the problem is. Ex. I'm always frustrated when [...]

**Describe the solution you'd like**
A clear and concise description of what you want to happen.

**Describe alternatives you've considered**
A clear and concise description of any alternative solutions or features you've considered.

**Additional context**
Add any other context or screenshots about the feature request here.
`,
			map[string][]string{},
		},
		{
			`**Describe the problem**

Please describe the issue you observed, and any steps we can take to reproduce it:

**To Reproduce**

What did you do? Describe in your own words.

If possible, provide steps to reproduce the behavior:

1. Set up CockroachDB cluster ...
2. Send SQL ... / CLI command ...
3. Look at UI / log file / client app ...
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Additional data / screenshots**
If the problem is SQL-related, include a copy of the SQL query and the schema
of the supporting tables.

If a node in your cluster encountered a fatal error, supply the contents of the
log directories (at minimum of the affected node(s), but preferably all nodes).

Note that log files can contain confidential information. Please continue
creating this issue, but contact support@cockroachlabs.com to submit the log
files in private.

If applicable, add screenshots to help explain your problem.

**Environment:**
 - CockroachDB version [e.g. 2.0.x]
 - Server OS: [e.g. Linux/Distrib]
 - Client app [e.g. ` + "`" + `cockroach sql` + "`" + `, JDBC, ...]

**Additional context**
What was the impact?

Add any other context about the problem here.`,
			map[string][]string{},
		},
		{
			`
**What is your situation?**

Select all that apply:

- is there a difference between the performance you expect and the performance you observe?
- do you want to improve the performance of your app?
- are you surprised by your performance results?
- are you comparing CockroachDB with some other database?
- another situation? Please explain.

**Observed performance**

What did you see? How did you measure it?

If you have already ran tests, include your test details here:

- which test code do you use?
- which SQL queries? Schema of supporting tables?
- how many clients per node?
- how many requests per client / per node?

**Application profile**

Performance depends on the application. Please help us understand how you use CockroachDB before we can discuss performance.

- Have you used other databases before? Or are you considering a migration? Please list your previous/other databases here.

- What is the scale of the application?
  - how many columns per table?
  - how many rows (approx) per table?
  - how much data?
  - how many clients? Requests / second?

- What is the query profile?
  - is this more a OLTP/CRUD workload? Or Analytics/OLAP? Is this hybrid/HTAP?
  - what is the ratio of reads to writes?
  - which queries are grouped together in transactions?

- What is the storage profile?
  - how many nodes?
  - how much storage?
  - how much data?
  - replication factor?

**Requested resolution**

When/how would you consider this issue resolved? 

Select all that applies:

- I mostly seek information: data, general advice, clarification.
- I seek guidance as to how to tune my application or CockroachDB deployment.
- I want CockroachDB to be optimized for my use case.
			`,
			map[string][]string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.body, func(t *testing.T) {
			ret := findTeamsFromKeywords(tc.body)
			require.Equal(t, tc.expected, ret)
		})
	}
}
