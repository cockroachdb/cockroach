---
name: 'Performance inquiry'
title: ''
about: 'You have a question about CockroachDB's performance and it is not a bug or a feature request'
labels: 'C-question'

---

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
