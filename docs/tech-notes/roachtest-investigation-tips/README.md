# Roachtest Investigation Tips

This directory contains guides for investigating common roachtest failures.

## General Investigation Workflow

1. **Gather artifacts** using `./scripts/fetch-roachtest-artifacts`.

2. **Identify the failure type** from test logs and error messages.

3. **Follow the appropriate guide** based on the failure type:
   - [**Debugging Job Failures**](debugging-a-job.md) - How to investigate failed or misbehaving
     jobs (backups, imports, CDC, etc.).
   - [**Node Exits, Crashes, and OOMs**](exits_crashes_ooms.md) - Investigating unexpected node
     exits and system-level failures.
   - [**Workload Failures**](workload-failure.md) - How to investigate failures originating from
     workload commands (use in concert with other guides).
   - [**Synthetic Failure Injection**](synthetic-failure-injection.md) - Investigating tests with
     network partitions, node kills, and other chaos engineering patterns (use in concert with other guides).

4. **Document findings**
   Edit and extend `investigation.md` to capture your findings:
   - Include citations to specific file locations or log files, or the grep/awk/etc command used to
     find them.
   - Update or add to the unified timeline as described below.
   - Before adding new sections to the investigation notes, review existing sections to see if
     there are already sections that should be amended or expanded instead.

5. **Review the code**: based on the observations above, perform a cursory review of code involved
   based on the errors, jobs, etc observed:
   - What are the major components, key functions, etc?
   - Are there conditionals or checks, settings or comments in the code that seem related?
   - What is the test trying to test? what is it doing to test that?
   - Map the call chain, request senders, component layers leading up to the site of failures to try
     to determine which layer or component was intended to be responsible for ensuring the condition
     that was ultimately violated.
      - In particular, is there logic specific to this which apparently failed? Or is it missing?

6. **Search git history** for recent changes related to the failure:
   - Check recent changes to the test: `git log --oneline -n 10 pkg/path/to/test`.
   - Check recent changes to relevant packages: `git log --oneline -n 20 pkg/path/to/relevant/package/`.
   - Look for related setting/feature/error names: `git log -S "keyword" --oneline`.
   
## Timeline Hygiene

Maintain a single, unified timeline for ease of reference throughout the investigation.
 - Use a consistent format: one line per entry, each prefixed with HH:MM:SS in UTC.
 - Do not create separate timelines for different components or log sources.
 - All events should be integrated into one chronological sequence.
 - Don't put too much detail in an entry; leave that to the detailed observation in the rest of the
   document.
   - For example, say you found hundreds of addsstable errors in the logs over 30 minutes during
     which a job makes no progress:
      Your timeline might show:
         `08:55:24 - job 456 records last progress change to 22% before stalling there for 34mins`
         `08:55:27 - first addsstable 'Command too large' error appears in logs on n2`
         `08:55:27-09:28:04 addsstable 'Command too large' errors on all nodes for 33mins`
      And there would be a section later in the document describing addsstable errors with complete
      examples, number, time period and location observed, etc.
- Distinguish test framework actions (network partitions, node kills) from cluster behavior
  responses.
- Note periods of baseline/normal behavior before or after issues to establish what changed, e.g.
  if a job progresses steadily at 1.5%/min for 20 min before stalling.
- Align test.log steps with node logs and system events in the same timeline.

## Systematic Event Correlation Analysis

When investigating performance changes or failures, use systematic correlation to identify what 
differs between working and non-working periods:

- **Identify transition periods**: Look for times when behavior changes (e.g., job progress rate 
  shifts, workload performance degrades, error patterns emerge).
- **Define before/after windows**: Establish specific time ranges for comparison (e.g., "at 11:32 
  we're going fast and at 11:37 we're slow").
- **Compare multiple dimensions systematically**:
  - Node distribution patterns (which nodes handle work before vs after)
  - Retry frequencies and error message patterns
  - Resource indicators (flow control blocking, admission control, memory pressure)
  - Infrastructure events (replica changes, range splits, rebalancing operations)
- **Look for timing correlations**: Document what events occurred during or just before transition 
  periods, but present findings as "timing correlations" rather than definitive causation.
- **Document pattern changes**: Describe how workload distribution, failure patterns, or 
  performance characteristics differ between periods.

This technique is particularly useful when investigating jobs that show changes in progress rates 
or workloads that show performance degradation over time.

## Related Tools

- `./scripts/fetch-roachtest-artifacts [issue_num|issue_link|issue_comment_link]` - Downloads test
  artifacts for investigation.
- `.claude/commands/roachtest-failure.md` - Claude Code automation instructions for following this
  guide.