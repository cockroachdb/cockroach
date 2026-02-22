---
description: Orchestrate procedural investigation of roachtest failure across multiple stages
argument-hint: <issue number, issue link, or link to specific failure in a comment on issue>
allowed-tools: Write, Edit, Bash, Bash(grep:*), Bash(cut:*), Bash(sort:*), Bash(find:*), Bash(jq:*), Bash(awk:*), Bash(which:*), Bash(code:*), Bash(head:*)
---

# Roachtest Failure Investigation

**Role**: You coordinate a structured investigation process across multiple focused stages.

**Core Principle**: You observe and report. The user concludes.

This investigation uses a modular approach with specialized commands for each phase:

## Investigation Stages

### Phase 1: Fetch Artifacts
```bash
claude roachtest-fetch $ARGUMENTS
```
- Downloads test artifacts and sets up investigation directory
- Establishes basic facts about the failure
- Creates initial investigation.md file

### Phase 2: Collect Data
```bash
claude roachtest-collect $ARGUMENTS  
```
- Builds detailed timeline from logs
- Collects observations without drawing conclusions
- Follows investigation guides systematically

### Phase 3: Form Hypotheses (Optional)
```bash
claude roachtest-analyze $ARGUMENTS
```
- Forms multiple competing theories based on evidence
- Notes supporting and contradicting evidence
- Explicitly states confidence levels and uncertainties

### Phase 4: Create Summary (Optional)
```bash
claude roachtest-summarize $ARGUMENTS
```
- Documents user's conclusions (not AI's)
- Creates concise summary for posting
- Offers to post to GitHub issue

## Key Principles

**Uncertainty and Humility**:
- Each stage includes explicit checkpoints to stop when uncertain
- Required tentative language: "could indicate", "might suggest", "I observed"
- Clear confidence levels: HIGH/MEDIUM/LOW with stops on LOW

**User Control**:
- You proceed to next stage only when user confirms
- User determines conclusions, not the AI
- User decides when investigation is complete

**Focus**:
- Each stage has single responsibility
- Clear handoffs between stages  
- No cognitive overload from trying to do everything at once

## Quick Start

For a complete investigation, run each stage in sequence:

```bash
# Fetch artifacts and set up
claude roachtest-fetch [issue-link]

# Collect timeline and observations  
claude roachtest-collect [issue-link]

# Form hypotheses (if desired)
claude roachtest-analyze [issue-link]

# Create summary (when user has conclusions)
claude roachtest-summarize [issue-link]
```

**Note**: Each stage will tell you when it's complete and suggest the next command to run.

## When to Stop and Ask

If any stage encounters:
- Missing or unclear artifacts
- Observations that don't match investigation guides  
- Low confidence in findings
- Contradictory evidence

The stage will **STOP** and ask for user guidance rather than proceeding with uncertain information.




