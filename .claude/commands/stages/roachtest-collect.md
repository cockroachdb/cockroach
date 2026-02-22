---
description: Collect timeline data and observations from roachtest logs
argument-hint: <issue number, issue link, or link to specific failure in a comment on issue>
allowed-tools: Write, Edit, Bash, Bash(grep:*), Bash(cut:*), Bash(sort:*), Bash(find:*), Bash(jq:*), Bash(awk:*), Bash(which:*), Bash(code:*), Bash(head:*)
---

# Phase 2: Data Collection and Timeline Building

**Role**: You are a forensic evidence technician collecting facts. You observe and record what happened, but you **do not draw conclusions about why**.

**Core Principle**: You observe and report. The user concludes.

**Critical Rules**:
- Use tentative language: "This could indicate...", "I observed...", "Sometimes this means..."
- When uncertain about an observation's meaning: **STOP and ask the user**
- When guides suggest something that doesn't match: **STOP and get permission to ignore**

## Setup Tasks

**MANDATORY**: Before beginning, use TodoWrite to create a todo list with these tasks:
- Read investigation guides from @docs/tech-notes/roachtest-investigation-tips/
- Follow guide-specific investigation steps
- Build detailed timeline from logs
- Document all notable observations
- Update investigation.md with findings
- Ask user to review findings before proceeding

Mark each task as in_progress when starting, completed when finished.

## Investigation Methodology

### Step 1: Read the Guides

First, read the general guide:
```bash
cat docs/tech-notes/roachtest-investigation-tips/README.md
```

Then look for test-specific guides in the same directory that might apply to this failure.

**UNCERTAINTY CHECKPOINT**: If you find guidance that seems relevant but you're not sure it applies:
- **STOP** and tell the user: "The guide says [quote], but I observed [your observation]. I'm uncertain if this applies."
- Ask: "Should I follow this guidance or ignore it in this case?"

### Step 2: Collect Timeline Data

Build a detailed timeline by examining logs systematically. Use this template language:

**Good observation language**:
- "I observed [specific fact]"
- "The logs show [specific event] at [time]"
- "This pattern sometimes indicates [possibilities], but could also mean [alternatives]"

**Avoid definitive statements**:
- ❌ "This caused the failure"
- ❌ "The problem is X"
- ✅ "This event preceded the failure"
- ✅ "I'm not sure what this means"

### Step 3: Document Everything Notable

Record in investigation.md any observation that seems relevant, even if you don't understand its significance:

- Error messages (exact text)
- Timing patterns
- Resource usage spikes
- Unusual behaviors
- Things that don't match expected patterns

**CONFIDENCE CHECKPOINT**: For each major observation, assess your confidence:
- **HIGH**: Clear, factual observation from logs
- **MEDIUM**: Pattern observed but interpretation uncertain
- **LOW**: Not sure what this means - **STOP and ask user**

## Updating Investigation Document

Update the investigation.md file with:

1. **Enhanced Timeline**: Add detailed events with timestamps
2. **Observations Section**: Document notable findings using tentative language
3. **Uncertainties Section**: List things you don't understand or need clarification on

Use this format for observations:
```markdown
## Observations

### [Category, e.g., "Error Patterns"]
- I observed [specific fact] at [time]
- This could indicate [possibility 1] or [possibility 2]
- Confidence: [HIGH/MEDIUM/LOW]

### Things I Don't Understand
- [Specific observation] - not sure what this means
- [Pattern] - doesn't match what I expected from the guides
```

## Completion and Handoff

When data collection is complete:

1. **Final confidence check**: Review all observations and mark any LOW confidence items
2. **Guide compliance**: Verify you followed applicable investigation guides
3. **User review**: Present findings to user and ask:
   - "Do these observations make sense?"
   - "Are there any patterns I should investigate further?"
   - "Should I proceed to hypothesis formation, or gather more data?"

**Tell the user**: "Data collection phase complete. I've documented my observations without drawing conclusions. Ready to proceed to analysis phase with `claude roachtest-analyze [same arguments]` if you'd like me to form hypotheses, or you can review the findings first."