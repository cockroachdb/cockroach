---
description: Collect information and present details of a roachtest failure
argument-hint: <issue number, issue link, or link to specific failure in a comment on issue>
allowed-tools: Write, Edit, Bash, Bash(grep:*), Bash(cut:*), Bash(sort:*), Bash(find:*), Bash(jq:*), Bash(awk:*), Bash(which:*), Bash(code:*), Bash(head:*)
---

Assist the user in running a procedural investigation of the roachtest failure for `$ARGUMENTS`,
focusing on helping establish and organize a timeline of events and findings.

This process has 4 distinct phases, each with a very specific role for AI assistants:
  - Phase 1: Setup: Gather the artifacts, and establish the basic facts.
  - Phase 2: Data Collection: Find notable events and behaviors and compile a timeline.
    - 'discovery phase': pure data collection with **no attempt to suggest a root cause**.
  - Phase 3: Hypothesis Formation, review and presentation.
  - Phase 4: User-determined Conclusions.

ROLE CLARITY: AI is performing the role of forensic evidence technician, NOT the detective solving 
the case.
- Technician: "The fingerprints are a match for suspect A" ✓
- Detective: "Suspect A committed the crime" ✗

In this exercise, the user plays the detective, not you, though in phase 3 (and only phase 3), you 
can assist them with ideas.

# Rule 0:

These rules apply at all times in all phases of this process:
  - Do NOT make definitive causal statements. Present evidence for a theory and let the user decide 
    if it proves it.
  - **FORBIDDEN**: You are PROHIBITED from using these phrases in any form unless explicitly told to
    by the user in Phase 4: "root cause" / "Root Cause" / "ROOT CAUSE", "the cause is" / "caused by".
  - Theories must be expressed in forms such as "could be explained by", "may indicate", etc. and 
    not as fact.

# Phase 1. Fetch the artifacts and perform basic triage

**MANDATORY**: Before beginning Phase 1, use TodoWrite to create a todo list with these tasks:
- Fetch artifacts using fetch-roachtest-artifacts script.
- Read test.log and failure_1.log to establish basic facts.
- Create investigation.md with basic facts and timeline.
- Open investigation file in VS Code (if available).
- Print summary and proceed to Phase 2.

Mark each task as in_progress when starting, completed when finished.

Fetch the test's outputs and artifacts, using the script that downloads and extracts them:

!./scripts/fetch-roachtest-artifacts $ARGUMENTS

**CHECKPOINT**: If the fetch fails or the investigation directory does not contain artifacts/test.log:
  - STOP. Show the script output. Link to the issue and ask the user what to do next. Do not try to
  proceed without artifacts or find your own artifacts.

Print the location of the investigation directory containing the fetched artifacts.

Read artifacts/test.log and artifacts/failure_1.log to establish basic facts:
- Test start time and duration before failure.
- The reported failure message (exact text).
- Find the direct cause of the test failure; if it timed out, what was it doing or waiting for when 
  it timed out.
  - Additional failure_n.log files may be interesting, but are often downstream failures, so focus 
    on the main log and first failure first.

If artifacts are missing or unable to establish these basics: STOP and ask the user for direction.

Create investigation.md in the investigation directory with:
- Test name, date, issue link, branch (e.g. master, release-x).
- The extracted reported failure reason from the test log or failure log.
- A 'Timeline' section with the initial timeline, including start, major test events, failure event, 
  and teardown.
  - Keep the timeline clean and easy to scan: line per entry, prefixed with the time (HH:MM:SS).
  - We'll update this timeline as we go later with more detail, rather than making several separate 
    timelines.
- An `Observations` section to be filled out in Phase 2.

Attempt to set the terminal window title to show the investigation name (best effort, ignore if fails):
Use the Bash tool to execute: `echo -ne "\033]0;Investigation: $(basename [ACTUAL_INVESTIGATION_PATH])\007" || true`

Attempt to open the investigation in VS Code (ignore if command fails due to `code` not being found):
Use the Bash tool to execute: `which code && code [ACTUAL_INVESTIGATION_PATH] && code [ACTUAL_INVESTIGATION_PATH]/investigation.md`

Print a summary and proceed directly to Phase 2.

# Phase 2. Collect information and Build Timeline

**MANDATORY**: Before beginning Phase 2, use TodoWrite to start the todo list for this phase:
  - 1. Re-read rule 0.
  - 2. Read the instructions for this phase.
  - 3. Read the general guide in @docs/tech-notes/roachtest-investigation-tips/README.md.
  - 4. Read any specialized guides for this kind of test and failure in
        @docs/tech-notes/roachtest-investigation-tips/.
  - 5. **TRANSITION**: Use TodoWrite to replace this setup list with a complete research plan 
        consisting of:
        - Re-reading rule 0 (if needed).
        - All investigation steps derived from the guides.
        - Review investigation.md to ensure it:
          a) complies with rule 0, 
          b) has a single, unified timeline as described in the readme
          c) has meged any duplicate or overlapping sections.
        - Proceed to Phase 3.

Mark each task as in_progress when starting, completed when finished.

In this phase your primary goal will be to **gather information** that could be relevant to the 
observed test result and behaviors, in the form of a timeline accompanied by sections on collected 
observations.

*Do not* try to establish a root cause, or suggest theories or reasons things happened in this phase.

As you collect data, if there is anything in the guides that appears related or relevant, but which
you think you can ignore or disregard, EXPLICITLY call this out: mention what the guide says and why
you think you should disregard or ignore it in this case, and then **STOP AND ASK** the user if they
agree: explain what the guide says, what you observed, and why it seems like it does not apply; let
them decide how to proceed.

As you find interesting observations -- not conclusions or theories, just observations -- add them to
the investigation document, adding new sections or amending existing ones as needed.

Be sure to record everything that is interesting or noted in this phase, even things which may not
be directly related to the error or a later hypothesis as to its cause.

Proceed to @docs/tech-notes/roachtest-investigation-tips/README.md.

# Phase 3. Analyze Gathered Information to Form Possible Hypotheses

**MANDATORY**: Before beginning Phase 3, use TodoWrite to start the todo list for this phase:
- Re-read rule 0.
- Construct multiple hypotheses based on evidence gathered in Phase 2.
- Review hypotheses against the investigation guides consulted in Phase 2.
- Update investigation.md with hypotheses section.
- Present findings and ask user for next steps.

Mark each task as in_progress when starting, completed when finished.

**CHECKPOINT**: Re-read Rule 0.

In this phase, we will:
  a. try to construct hypotheses about what happened.
  b. review hypotheses against observations and determine if we need more observations.
  c. present and discuss hypotheses with the user.
 
## A: Construct Hypothesis

Construct -- but do not yet present -- multiple alternative hypotheses that can explain observed 
failures, errors, and behaviors.
- Note observations support or contradict each.
- ALWAYS ask yourself "if the most likely theory is wrong, what's the next most likely possibility?", 
  then explore it as well.
- Note which hypothesis appears most compatible with observations, but avoid overconfident conclusions.
  - *Never* claim something is *the* problem or *the root cause*.
- Never state inferred conclusions as definitive fact: present observations and your reasoning and 
  let the user draw conclusions.
- Identify any observations or behaviors which are explained by the proposed hypotheses.

## B. Review Hypotheses and Observations

Re-read all applicable guides mentioned in Phase 2 and compare them to your hypotheses.
- Identify and quote anything in the guide that relates to observed behavior.
- Explicitly note when the guide either confirms or contradicts a hypothesis.
- If a hypothesis relies on ignoring, disregarding, or contradicting something in the guide:
  - **STOP**: Explain to the user why you think you should ignore the normal guidance and 
    **get permission** to continue with that hypothesis.

Given the hypotheses constructed, does it make sense to go back to Phase 2 with an expanded search?
- Would we find more evidence to support a hypothesis?
- Could we find evidence to contradict one?
- If we have unexplained observations that do not fit with any hypotheses, can we find anything 
  related to them?
- Should we search for additional terms, ranges, events, or time periods in the logs to add support 
  for or refute a hypothesis?
- Is there anything in the system tables or configuration that is relevant to any of our hypotheses?
- Can we find and link to relevant areas of the code base?

## C. Present Hypotheses

**STOP: MANDATORY REVIEW CHECKPOINT:**
Read and then actively confirm you have verified you are complying with each point in this checkpoint:
  - Re-read Rule 0.
  - Review what you are about to present: Are you presenting theories and evidence, not conclusions?
  - Are you being sure to highlight anything that does not make sense, does not support your 
    theories, or is contradictory?

**CRITICAL** DO NOT claim to have found a root cause or state one in the investigation notes until 
told to explicitly by the user.

### c1. Update the Document

Add (or update) the 'Hypotheses' section at the end of investigation document with your proposed
hypotheses, noting supporting and contradicting evidence, and explicitly highlighting any
contradictions, unexplained unusual observations and open questions.

### c2. Present Hypotheses

Present your hypotheses to the user, noting supporting and contradicting evidence, and explicitly 
highlighting any contradictions, unexplained unusual observations and open questions.

### c3. Summarize and Determine Next Steps

Briefly recap:
  - The full path to our full investigation notes file.
  - What we see and our observations (i.e. The Facts).
  - What we think might be happening and why (i.e. The Possibilities).
  - What we don't understand and could look into next (i.e. The Questions).

Ask the user what to do next.

If the user asks follow-up or clarifying questions:
- Check investigation.md first: often they are asking about something already mentioned in the
  findings which can be more quickly answered without a lengthy reexamination of the logs and
  artifacts.
- If they ask *why* something happened, remember rule 0 and respond with possibilities and
  corroborating evidence, not authoritative conclusions.

When answering user questions, particularly if the answer required additional search and analysis
of the test artifacts, consider updating the investigation notes to incorporate additional
information: the fact the user asked about it suggests the answer may be useful information to have
reflected in the notes, either in an additional section or timeline entry or as a correction or
added detail in an existing section as appropriate. The notes document is a living document to be
updated as we learn more.

If the user suggests additional research, return to phase 2, incorporating their added directions.
If the user states that the investigation has concluded, proceed to Phase 4.

# Phase 4. Wrap Up

**MANDATORY REVIEW CHECKPOINT**
  - Did the user say that the investigation is complete? If not, **STOP**: GO BACK to Phase 3.
  - Re-read Rule 0.
  - Do not offer judgments on impact, severity, or recommendations.
  - Use TodoWrite to update the todo list:
    - Update investigation notes.
    - Produce separate, short summary.
    - Review the guide books for potential improvements.

Update the investigation document:
  - If the user stated something is the root cause, add this conclusion to the document.
    - If they did not explicitly state that something is the root cause but you inferred it, STOP
      and ask: "Did you conclude that X is the root cause? Should I add this to the document?"
  
Summarize the results in a ready-to-post markdown comment:
  - Aim for ~20 lines or less if possible: condensed timeline, user's chosen root-cause or hypotheses 
    and its key supporting observations.
  - If the user asks, post this to the github issue as a comment using `gh`; otherwise write it to 
    summary.md.
  - If the user says to close the issue with a comment, use `gh issue close --comment`.

Review the guides in @docs/tech-notes/roachtest-investigation-tips for gaps highlighted by this 
investigation.
  - Consider the purpose of the guides:
    - These guides are here to make the process of investigating a test more consistent, reliable, 
      and easier (in that order).
    - These guides are meant to be reusable across different tests, in the face of different, 
      sometimes novel failures.
    - These guides should present information on behavior of the systems and its tests, along 
      strategies for inspecting it.
    - The guides should not be narrowly specific to a single bug or test, and prone to becoming out 
      of date quickly.
    - These guides are also used against different versions of the same tests on different branches, 
      where specific test names or behaviors can differ.
    - Examples:
      - Good: "Many schema changes make additional, secondary 'schema change GC' jobs that will 
        remain as "running" after the operation completes; these are normal."
      - Bad: "failed to dial node 4 on line 456 of the foobar test means it crashed."
  - Consider places in this investigation, if any, where user direction was required due to gaps in 
    the guidance:
    - Did the user need to steer the investigation towards something it didn't capture on its own? 
      away from something that wasn't helpful?
    - Did the user need to correct any misunderstandings of the observations or behaviors?
    - If so, propose areas to expand or revise, with specific edits you would make.




