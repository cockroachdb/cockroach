---
name: jira-internal-task
description: Create Jira Internal tasks for tracking reactive work items. Use when the user asks to create a Jira task, internal task, or add something to Jira.
---

# Jira Internal Task Creator

Help the user create Jira "Internal tasks" for tracking reactive work items on sprint boards.

## When to Use

This skill activates when the user says:
- "make an internal task for this"
- "create an internal task to do X"
- "add this to my jira"
- "create this on jira"
- "make jira task"
- Any similar phrasing about creating Jira tasks or internal tasks

## What are Internal Tasks?

Internal tasks are brief work items for sprint tracking:
- **Audience**: Primarily the engineer and their manager
- **Purpose**: Track "reactive" work items on sprint boards
- **Scope**: Don't need to be exhaustive or fully self-contained
- **Detail level**: Less important that someone unfamiliar can fully educate themselves from the issue

## Workflow

1. **Understand the request**: Parse what the user wants to track
2. **Draft title and description**:
   - Title should be concise and actionable
   - Description can be brief but should capture key context
   - If unclear, ask for clarification or propose a title/description for approval
3. **Gather metadata**:
   - Parent issue key (typically CRDB-XXXXX format, optional). When the user mentions an epic by number only (e.g., "epic 12345" or "under 12345"), infer the full format as CRDB-12345.
   - Sprint assignment (see Sprint Handling below)
   - If the user mentions a team different from their default (e.g., "for the SQL team"), pass `--eng-team` explicitly
4. **Get user approval**: Show the proposed title and description before creating
5. **Create the task**: Use the Go tool with explicit `--sprint` flag

## Content Guidelines

**Title:**
- Short, actionable statement
- Start with a verb when possible (e.g., "Investigate", "Fix", "Add", "Update")
- Keep under 100 characters

**Description:**
- Brief context about what needs to be done
- Can reference specific code, issues, or conversations
- Don't need extensive background - this is for tracking, not documentation
- 1-3 paragraphs is usually sufficient

**Be lenient about scope** - these are quick tracking items, not full specifications. However:
- If completely unclear what the user wants, ask for clarification
- If you can infer a reasonable title/description, propose it for approval

## Script Usage

The skill uses a Go tool at `.claude/skills/jira-internal-task/cmd/jira-task/`:

```bash
# List sprints (active + future, excluding closed) - safe to run for sanity checks
cd .claude/skills/jira-internal-task && go run ./cmd/jira-task sprints

# Create an internal task (--sprint and --assignee are REQUIRED)
cd .claude/skills/jira-internal-task && go run ./cmd/jira-task create \
  --title "Brief task title" \
  --description "Task description with context" \
  --sprint <numeric-id|latest|-1> \
  --assignee <email|unassigned> \
  [--parent CRDB-12345]
```

## Sprint Handling (IMPORTANT)

The `--sprint` flag is **required** and must be passed explicitly. The agent must always ask the user to decide sprint behavior:

1. **Preferred: Use a numeric sprint ID** - Run `jira-task sprints` to list available sprints and pick the appropriate ID
2. **Use `latest`** - Only if the user explicitly says "current sprint" or "latest sprint"
3. **Use `-1` (backlog)** - If the user says "backlog", "put it in the backlog", "in the backlog", or otherwise indicates they don't want it in any sprint

**Always ask the user** which sprint to use. Never assume a default. Example prompt:
> "Which sprint should this go in? I can put it in the current active sprint, a specific sprint (I can list them), or the backlog (no sprint)."

## Task Parameters

- `--title`: Required. The task title
- `--description`: Required. The task description/body
- `--sprint`: **Required**. Numeric sprint ID (preferred), `latest`, or `-1` for backlog (no sprint)
- `--parent`: Optional. Parent issue key (e.g., CRDB-12345)
- `--assignee`: **Required**. Email address of the assignee, or `"unassigned"` to create without an assignee
- `--project`: Defaults to `CRDB`

**Auth Parameters (from environment—never pass explicitly):**
- `--token`: From `$JIRA_TOKEN`
- `--email`: From `$JIRA_EMAIL`

**Team Parameters (from environment, override if user specifies their team):**
- `--eng-team`: From `$JIRA_ENG_TEAM`. If the user mentions their team (e.g., "I'm on SQL"), pass it explicitly.
- `--board-id`: From `$JIRA_<ENG_TEAM>_BOARD_ID` (e.g., `$JIRA_KV_BOARD_ID`)

**IMPORTANT**:
- This tool ONLY creates "Internal Task" issue types
- Never send API requests directly—always use the tool
- **During development/testing, NEVER actually create Jira tickets**
- You MAY run `jira-task sprints` for sanity checks (it has no side effects)

## Example Interaction

**User:** "Make an internal task to investigate the query optimizer slowdown we saw this morning"

**Assistant:**
I'll create a Jira internal task for this. Here's what I'm proposing:

**Title:** Investigate query optimizer slowdown
**Description:**
Investigate the query optimizer performance degradation observed this morning. Need to identify the cause and potential fixes.

Which sprint should this go in?
- Current active sprint (`latest`)
- A specific sprint (I can list available sprints)
- Backlog (no sprint assignment)

Do you also want to link this to a parent epic? (provide CRDB-XXXXX key if so)

[User specifies sprint preference]

**Assistant:** [Runs the tool with explicit --sprint flag]

## Resolving Assignees

When the user says "for wenyi@", "assigned to wenyi", or similar, **always verify** the email by searching git history (since Jira silently ignores invalid assignees):

```bash
git log --since="3 months ago" --format='%an <%ae>' | sort -u | grep -i "<name>"
```

- **Single match**: Use that email
- **Multiple matches**: Ask the user to clarify (e.g., "Did you mean nick@ or nicholas@?")
- **No matches**: Ask the user for the full email address

Examples:
- "for wenyi" → search git log, find `Wenyi Hu <wenyi@cockroachlabs.com>`, use `wenyi@cockroachlabs.com`
- "assign to nick" → search git log, find `Nick Travers <nick@cockroachlabs.com>`, use `nick@cockroachlabs.com`
- "for john" → if multiple Johns found, ask: "Did you mean john.smith@ or john.doe@?"

## Error Handling

- If the tool fails, report the error to the user
- If required parameters are missing, ask the user
- If the request is ambiguous, propose a draft and get approval

## Notes

- These tasks are meant to be lightweight - don't overthink the content
- Focus on capturing enough info for the engineer to remember context
- When in doubt, ask the user rather than creating something unclear
