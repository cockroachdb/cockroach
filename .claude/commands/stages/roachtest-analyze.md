---
description: Form hypotheses about roachtest failure based on collected evidence
argument-hint: <issue number, issue link, or link to specific failure in a comment on issue>
allowed-tools: Write, Edit, Bash, Bash(grep:*), Bash(cut:*), Bash(sort:*), Bash(find:*), Bash(jq:*), Bash(awk:*), Bash(which:*), Bash(code:*), Bash(head:*)
---

# Phase 3: Hypothesis Formation and Analysis

**Role**: You help form theories about what might have happened, but you **do not determine the actual cause**.

**Core Principle**: You propose possibilities and present evidence. The user decides what's true.

**Required Language**:
- "This could be explained by..."
- "One possible hypothesis is..."
- "The evidence might suggest..."
- "I'm uncertain about..."

**NEVER say**: "the cause is", "caused by", "the problem is", "the root cause"

## Setup Tasks

**MANDATORY**: Before beginning, use TodoWrite to create a todo list with these tasks:
- Review collected evidence from investigation.md
- Form multiple competing hypotheses
- Check hypotheses against investigation guides
- Identify supporting and contradicting evidence
- Update investigation.md with hypotheses section
- Present findings to user with explicit uncertainties

Mark each task as in_progress when starting, completed when finished.

## Hypothesis Formation

### Step 1: Review Evidence

Read the investigation.md file to understand what was observed in Phase 2.

**CONFIDENCE CHECK**: If you feel the evidence is insufficient or unclear:
- **STOP** and tell the user: "The evidence collected seems insufficient to form reliable hypotheses"
- Suggest returning to data collection phase
- Ask: "Should I gather more specific data before forming theories?"

### Step 2: Form Multiple Hypotheses

**REQUIRED**: You must form at least 2-3 different hypotheses. Always ask yourself: "If my most likely theory is wrong, what else could explain this?"

For each hypothesis, document:
1. **What it proposes**: Brief description of the theory
2. **Supporting evidence**: Observations that fit this theory
3. **Contradicting evidence**: Observations that don't fit
4. **Confidence level**: HIGH/MEDIUM/LOW
5. **What's still unknown**: Questions this theory doesn't answer

**Template for each hypothesis**:
```markdown
### Hypothesis [N]: [Brief description]

**Confidence**: [HIGH/MEDIUM/LOW]

**This theory could explain**:
- [Supporting observation 1]
- [Supporting observation 2]

**This theory struggles to explain**:
- [Contradicting observation 1]
- [Contradicting observation 2]

**Still unknown**:
- [Unanswered question 1]
- [Unanswered question 2]
```

### Step 3: Reality Check Against Guides

Compare your hypotheses against the investigation guides:

- Does anything in the guides contradict your theories?
- Do the guides suggest other possibilities you missed?

**GUIDE CONFLICT CHECKPOINT**: If your hypothesis contradicts guidance:
- **STOP** and tell the user: "My hypothesis contradicts the guide which says [quote]"
- Explain: "I think this might not apply because [reasoning]"
- Ask: "Should I disregard the guide in this case, or revise my hypothesis?"

### Step 4: Confidence Assessment

For each hypothesis, honestly assess your confidence:

- **HIGH** (70%+): Strong evidence, few contradictions
- **MEDIUM** (40-70%): Some evidence, some uncertainties
- **LOW** (<40%): Weak evidence, many unknowns

**LOW CONFIDENCE CHECKPOINT**: If all hypotheses are LOW confidence:
- **STOP** and tell the user: "I have multiple theories but low confidence in all of them"
- List the major uncertainties
- Ask: "Should I gather more specific evidence before continuing?"

## Update Investigation Document

Add a "Hypotheses" section to investigation.md:

```markdown
## Hypotheses

**Note**: These are theories based on available evidence. The user determines which, if any, is correct.

[Use the template above for each hypothesis]

## Unresolved Questions
- [Question 1 that none of the hypotheses fully answer]
- [Question 2 that needs more investigation]

## Recommendation
Based on the evidence and my confidence levels, I recommend [specific next steps or additional investigation].
```

## Present Findings

Present your analysis to the user with explicit uncertainty:

1. **Summarize**: "Based on the evidence, I've formed [N] hypotheses"
2. **Highlight uncertainties**: "I'm most uncertain about [specific aspects]"
3. **Note contradictions**: "Some observations don't fit any theory well"
4. **Ask for guidance**: "Which direction should we explore further?"

**Tell the user**: "Hypothesis formation complete. I've proposed [N] theories with varying confidence levels. You decide which seem most promising or if we need more evidence. Ready for you to draw conclusions or proceed to summary phase."