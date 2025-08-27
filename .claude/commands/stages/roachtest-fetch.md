---
description: Fetch roachtest artifacts and set up investigation directory
argument-hint: <issue number, issue link, or link to specific failure in a comment on issue>
allowed-tools: Write, Edit, Bash, Bash(grep:*), Bash(cut:*), Bash(sort:*), Bash(find:*), Bash(jq:*), Bash(awk:*), Bash(which:*), Bash(code:*), Bash(head:*)
---

# Phase 1: Fetch Artifacts and Basic Setup

**Role**: You are setting up the investigation workspace. Focus on getting artifacts and establishing basic facts.

**Core Principle**: If something doesn't work or artifacts are missing, **STOP and ask the user** rather than guessing or improvising.

## Setup Tasks

**MANDATORY**: Before beginning, use TodoWrite to create a todo list with these tasks:
- Fetch artifacts using fetch-roachtest-artifacts script
- Verify artifacts are complete
- Read test.log and failure_1.log to establish basic facts
- Create investigation.md with basic timeline
- Open investigation file in VS Code (if available)
- Print summary of what was found

Mark each task as in_progress when starting, completed when finished.

## Fetch Artifacts

Run the script to download and extract test artifacts:

```bash
./scripts/fetch-roachtest-artifacts $ARGUMENTS
```

**CRITICAL CHECKPOINT**: If the fetch fails or the investigation directory does not contain `artifacts/test.log`:
  - **STOP IMMEDIATELY**
  - Show the script output to the user
  - Say: "The artifact fetch failed or is incomplete. I cannot proceed without artifacts."
  - Ask the user what to do next
  - **Do not try to find your own artifacts or proceed without them**

Print the location of the investigation directory containing the fetched artifacts.

## Establish Basic Facts

Read `artifacts/test.log` and `artifacts/failure_1.log` to determine:

1. **Test start time and duration** before failure
2. **The reported failure message** (exact text from logs)
3. **What the test was doing** when it failed (if it timed out, what was it waiting for?)

**UNCERTAINTY CHECKPOINT**: If any of these basic facts are unclear or missing:
  - **STOP** and tell the user: "I cannot clearly determine [specific fact] from the logs"
  - Show what you found and what you expected to find
  - Ask for guidance before proceeding

## Create Investigation Document

Create `investigation.md` in the investigation directory with:

```markdown
# Roachtest Investigation: [TEST_NAME]

**Issue**: [ISSUE_LINK]
**Date**: [DATE]
**Branch**: [BRANCH]

## Reported Failure
[Exact failure message from logs]

## Timeline
- [HH:MM:SS] Test started
- [HH:MM:SS] [Major test event if found]
- [HH:MM:SS] **FAILURE**: [Failure description]
- [HH:MM:SS] Test ended

## Observations
[To be filled in Phase 2]

## Hypotheses
[To be filled in Phase 3]
```

## Setup Development Environment

Attempt to set terminal title (ignore if fails):
```bash
echo -ne "\033]0;Investigation: $(basename [INVESTIGATION_PATH])\007" || true
```

Attempt to open in VS Code (ignore if `code` command not found):
```bash
which code && code [INVESTIGATION_PATH] && code [INVESTIGATION_PATH]/investigation.md
```

## Summary and Next Steps

Print a brief summary:
- Investigation directory location
- Basic facts established (test name, failure type, timing)
- What's ready for the next phase

**Tell the user**: "Phase 1 complete. Artifacts fetched and basic investigation document created. Ready to proceed to data collection phase with `claude roachtest-collect [same arguments]`."