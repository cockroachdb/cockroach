---
name: 'Bug or crash report'
title: ''
about: 'Report unexpected behavior to help us improve'
labels: 'C-bug'
assignees: ''
---

**Describe the problem**

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
 - Client app [e.g. `cockroach sql`, JDBC, ...]

**Additional context**
What was the impact?

Add any other context about the problem here.
