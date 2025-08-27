---
description: Create final summary of roachtest investigation based on user conclusions
argument-hint: <issue number, issue link, or link to specific failure in a comment on issue>
allowed-tools: Write, Edit, Bash, Bash(grep:*), Bash(cut:*), Bash(sort:*), Bash(find:*), Bash(jq:*), Bash(awk:*), Bash(which:*), Bash(code:*), Bash(head:*)
---

# Phase 4: Wrap Up and Summary

**Role**: You create summaries based on the user's conclusions, not your own.

**Core Principle**: The user determines what happened. You document their conclusions and create summaries.

**CRITICAL**: Only proceed if the user has explicitly stated their conclusions about the investigation.

## Prerequisites Check

**MANDATORY CHECKPOINT**: Before beginning, verify:
- Has the user explicitly stated what they believe caused the failure?
- OR has the user said the investigation is complete without finding a cause?
- OR has the user asked to close the investigation?

**If NO to all**: **STOP** and tell the user: "I need you to provide your conclusions before I can create a summary. What do you think happened?"

## Setup Tasks

**MANDATORY**: Before beginning, use TodoWrite to create a todo list with these tasks:
- Confirm user's stated conclusions
- Update investigation.md with user's determination
- Create concise summary for posting
- Review investigation guides for improvements
- Offer to post summary or close issue if requested

Mark each task as in_progress when starting, completed when finished.

## Document User Conclusions

Update the investigation.md file with a new section:

```markdown
## User Conclusions

**Determined by**: [User name/role]
**Date**: [Current date]

**Root cause determination**: [User's stated conclusion]

**Reasoning**: [User's explanation of why they reached this conclusion]

**Confidence**: [If user provided this]
```

**VERIFICATION CHECKPOINT**: Before adding conclusions, confirm with user:
- "You stated that [restate their conclusion]. Should I document this as your determination?"
- If they said it's inconclusive: "You determined that the evidence is inconclusive. Should I document that?"

## Create Summary

Generate a concise summary (aim for ~20 lines) suitable for posting:

```markdown
## Investigation Summary

**Test**: [Test name]
**Failure**: [Brief failure description]
**Investigation period**: [Time span]

### Key Timeline
- [Time]: [Key event 1]
- [Time]: [Key event 2]
- [Time]: **FAILURE**: [Failure event]

### Key Observations
- [Most important finding 1]
- [Most important finding 2]

### Conclusion
Based on investigation, [user's determination].

### Supporting Evidence
- [Key evidence that supports the conclusion]
```

Write this summary to `summary.md` in the investigation directory.

## Offer Actions

Ask the user what they'd like to do:

1. **Post to GitHub**: "Should I post this summary as a comment on the issue using `gh issue comment`?"
2. **Close Issue**: "Should I close the issue with this comment using `gh issue close --comment`?"
3. **Just Save**: "Summary saved to summary.md for your use"

## Review Guide Effectiveness

Briefly review the guides used in this investigation:

**Consider**:
- Did the guides help identify the issue quickly?
- Were there gaps where user direction was needed?
- Did any guide suggest something that proved unhelpful?

**If gaps identified**, suggest to user:
- "The investigation guides might benefit from [specific improvement]"
- "Should I suggest updates to the guide maintainers?"

## Complete Investigation

**Final confirmation**:
- Investigation directory: [full path]
- Summary file: [full path to summary.md]
- User conclusion: [their determination]

**Tell the user**: "Investigation complete. Summary created based on your conclusion that [restate conclusion]. All files saved in [directory path]."