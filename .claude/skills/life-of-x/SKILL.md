---
name: life-of-x
description: Generate comprehensive "Life of X" documentation by tracing code flow through a codebase with accurate GitHub links using current SHA
---

# Life of X Documentation Generator

You are helping the user create comprehensive documentation that traces the complete flow of a concept, request, or operation through a codebase, similar to the "Life of a Query" documentation style. Every code link MUST use the current git SHA, not hardcoded or guessed values.

## Prerequisites

Before starting, verify:
1. Current directory is a git repository: `git rev-parse --git-dir`
2. Repository has a remote: `git remote get-url origin`
3. Working directory state (warn if uncommitted changes): `git status --short`

If any prerequisite fails, stop and inform the user.

## Step 1: Gather Repository Information

Execute these commands and store the results:

```bash
# Get current commit SHA
SHA=$(git rev-parse HEAD)
echo "Current SHA: $SHA"

# Get repository URL and normalize it
REMOTE_URL=$(git remote get-url origin)
# Convert SSH to HTTPS format if needed
REPO_URL=$(echo "$REMOTE_URL" | sed 's/git@github.com:/https:\/\/github.com\//' | sed 's/\.git$//')
echo "Repository URL: $REPO_URL"

# Get repository root path
REPO_ROOT=$(git rev-parse --show-toplevel)
echo "Repository root: $REPO_ROOT"

# Get current branch
BRANCH=$(git branch --show-current)
echo "Current branch: $BRANCH"
```

## Step 2: Collect User Requirements

Use AskUserQuestion to gather:

1. **What to trace** (header: "Concept"):
   - Options: ["HTTP request", "Database query", "Authentication flow", "Message processing"]
   - Allow custom input via "Other"

2. **Entry point location** (header: "Entry"):
   - Ask: "Where does the flow begin? (e.g., main.py:handle_request, app.js, or describe the component)"

3. **Documentation scope** (header: "Scope"):
   - Options: ["Complete flow with all details", "Main path only", "Include error handling", "Focus on specific subsystem"]

4. **Output filename** (header: "Output"):
   - Ask: "Output filename? (default: life_of_[concept].md)"

## Step 3: Locate Entry Points

Based on user input, find the starting point:

```bash
# Search for the entry point (adjust pattern based on language)
# For a function/method name:
git grep -n "def $ENTRY_NAME\|function $ENTRY_NAME\|func $ENTRY_NAME\|class $ENTRY_NAME" --max-count=5

# For a general component search:
git grep -n "$COMPONENT_NAME" --max-count=10 | head -20
```

For each potential entry point found:
1. Show the file path and line number
2. Read a few lines of context
3. If multiple matches, ask user to select the correct one

## Step 4: Trace Code Flow

Starting from the entry point, systematically trace the execution:

### 4.1 Read Entry Point File
```bash
# Get the entry point file content
cat "$ENTRY_FILE"
```

Identify:
- Function/method calls
- Class instantiations
- Module imports
- Return values and data flow

### 4.2 Follow Each Call
For each function call found:

```bash
# Find the definition
git grep -n "def $FUNCTION_NAME\|function $FUNCTION_NAME\|func $FUNCTION_NAME" --max-count=3

# Get line number for link generation
LINE_NUM=$(git grep -n "def $FUNCTION_NAME" "$FILE" | cut -d: -f1 | head -1)
```

Generate link: `${REPO_URL}/blob/${SHA}/${FILE_PATH}#L${LINE_NUM}`

### 4.3 Build Call Graph
Track:
- Call hierarchy (what calls what)
- Data transformations
- State changes
- Cross-module boundaries
- External service calls

Limit depth to 10 levels to avoid infinite recursion.

## Step 5: Generate Documentation

Create the output document with this structure:

```markdown
# Life of a [X]

Generated: [DATE]
Repository: [REPO_URL]
Commit: [SHA]

## Introduction

This document traces the execution of [X] through the [PROJECT] codebase,
following the code paths through various system layers.

## Entry Point

The flow begins at [`entry_function`](LINK) in `path/to/file.ext`:

[Brief description of what happens at entry]

## Phase 1: [Initial Processing]

### [Component Name]

The [`ClassName`](LINK) handles [responsibility]. Starting at [`method`](LINK):

\```language
// 2-3 lines of actual code for context
\```

This calls [`next_function`](LINK) which...

## Phase 2: [Core Logic]

[Continue pattern...]

## Data Flow

1. Input arrives as [format] at [`entry`](LINK)
2. Transformed to [format] by [`processor`](LINK)
3. Stored in [structure] at [`storage`](LINK)
4. Returned as [format] from [`output`](LINK)

## Error Handling

- [Error Type]: Handled at [`error_handler`](LINK)
- [Exception]: Caught and processed at [`catch_block`](LINK)

## Summary

The complete flow traverses [N] components across [M] files:
- Total lines of code in flow: [COUNT]
- Key decision points: [COUNT]
- External service calls: [COUNT]
```

## Step 6: Generate and Validate Links

For EVERY code reference, generate a GitHub link:

```bash
# Get exact line number
LINE=$(git grep -n "exact_code_pattern" "$FILE" | cut -d: -f1 | head -1)

# Generate link
LINK="${REPO_URL}/blob/${SHA}/${FILE_PATH}#L${LINE}"

# For line ranges
LINK="${REPO_URL}/blob/${SHA}/${FILE_PATH}#L${START}-L${END}"
```

Validate each link:
1. Ensure file exists: `test -f "$REPO_ROOT/$FILE_PATH"`
2. Verify line number is valid
3. Check that SHA is current: `git rev-parse HEAD`

## Step 7: Write Output

Save the generated documentation:

```bash
# Write to file
cat > "$OUTPUT_FILE" << 'EOF'
[Generated documentation content]
EOF

echo "Documentation generated: $OUTPUT_FILE"
echo "Total files analyzed: $(count)"
echo "Total code links generated: $(count)"
```

## Error Handling

### Not a Git Repository
```bash
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "Error: Not a git repository. Please run from within a git repository."
    exit 1
fi
```

### No Remote Repository
```bash
if ! git remote get-url origin > /dev/null 2>&1; then
    echo "Error: No remote repository configured. Cannot generate GitHub links."
    echo "Add a remote with: git remote add origin <url>"
    exit 1
fi
```

### File Not Found
When a referenced file doesn't exist, note it in the documentation:
```markdown
> Note: Referenced file not found in current commit
```

### Circular Dependencies
Track visited functions in a set. If revisiting, note the cycle:
```markdown
> Circular reference: function calls itself via [path]
```

## Important Notes

1. **ALWAYS use current SHA** - Never hardcode or guess commit hashes
2. **Verify file existence** - Check files exist before generating links
3. **Handle multiple languages** - Adapt search patterns to file extensions
4. **Respect scope** - Stay within user-defined boundaries
5. **Include context** - Show 2-3 lines of code for each reference
6. **Track recursion** - Limit depth to prevent infinite loops
7. **Generate valid URLs** - Ensure special characters are properly encoded

## Completion

Report to user:
- Output file location
- Statistics (files analyzed, links generated, depth reached)
- Any warnings or limitations encountered
- Suggestions for manual review points
