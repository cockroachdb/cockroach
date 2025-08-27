# Investigating Workload Failures

Use when test failure originates from workload commands (not test framework).

Workloads are run as part of larger tests: this guide has tips for investigating the workload
failure in particular, but should be used *in conjunction with* the guide for the applicable test.

Sometimes the failure of the workload points to a problem in the feature being tested in the test;
that is, after all, why the workload was being used in the test in the first place -- to catch bugs.

However sometimes the workload fails for unrelated reasons, due to bugs in the workload itself or in
the test setup.

It is important to collect evidence which can differentiate these cases.

## Investigation Steps

### Read the workload logs:
```bash
# Get workload configuration
grep "workload.*run" test.log
```
- Trace operation sequence leading to failure.
- Check if workload validates preconditions.

### Analyze Scope

Consider what the workload was operating on: was it running on tables or database read by the
tested operation, or written by it? The latter makes it more likely the operation is suspect, e.g.
if a workload is running on a restored table and crashes, maybe the restore operation restored the
table incorrectly. But if a workload is running on a table that is backed up only to be restored
elsewhere, then it is unlikely (but not impossible), that the backup of the table caused the
workload running on that table to fail.

# TODO: schemachange

# TODO: randomsyntax
