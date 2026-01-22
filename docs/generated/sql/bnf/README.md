# SQL Diagram Guide for Writers

This guide explains how to add or modify SQL railroad diagrams in CockroachDB documentation.

## Quick Start

**You only need to edit one file:**
```
pkg/cmd/docgen/diagrams.go
```

Everything else is automated by CI.

## What Gets Automated

When you open a PR that modifies `diagrams.go`:

1. CI generates BNF grammar files from `sql.y`
2. CI builds HTML/SVG railroad diagrams
3. CI runs validation tests (catches broken links before merge)
4. PNG previews appear in your PR comment
5. A bot creates a PR in `cockroachdb/generated-diagrams` to sync the files

## How to Add or Modify a Diagram

### Step 1: Edit diagrams.go

Each diagram is defined as a spec. Here's an example:

```go
{
    name:    "drop_database",
    stmt:    "drop_database_stmt",
    inline:  []string{"opt_drop_behavior"},
    replace: map[string]string{"name": "database_name"},
    unlink:  []string{"database_name"},
},
```

### Step 2: Open a PR

Create a branch, make your changes, and open a PR against `master`.

### Step 3: Review the PNG Preview

The CI will post a comment on your PR with PNG previews of the diagrams. Check that they look correct.

### Step 4: Add remote_include (New Diagrams Only)

For **new** diagrams, add this to the docs page where you want it to appear:

```markdown
{% remote_include https://raw.githubusercontent.com/cockroachdb/generated-diagrams/master/grammar_svg/your_diagram_name.html %}
```

Existing diagrams already have their includes in place.

## Diagram Spec Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique name for the diagram (used in filenames) |
| `stmt` | No | Grammar production to use (defaults to `name` if empty) |
| `inline` | No | Productions to expand inline in the diagram |
| `replace` | No | Text replacements to make labels clearer |
| `unlink` | No | Tokens to remove hyperlinks from |
| `match` | No | Regex patterns the production must match |
| `exclude` | No | Regex patterns to exclude from the diagram |
| `nosplit` | No | If true, keep diagram on one line |

## Common Tasks

### Make a label clearer

If the diagram shows a confusing grammar term like `a_expr`, replace it:

```go
replace: map[string]string{"a_expr": "job_id"},
unlink:  []string{"job_id"},
```

### Expand a sub-production

If part of the diagram is collapsed and should be shown:

```go
inline: []string{"opt_with_options", "option_list"},
```

### Remove a confusing branch

If the diagram shows internal syntax that users don't need:

```go
exclude: []*regexp.Regexp{regexp.MustCompile("'INTERNAL_KEYWORD'")},
```

## Understanding Diagram Colors

The railroad diagrams use standard notation:

- **Yellow boxes**: Keywords you type literally (e.g., `SELECT`, `DROP`)
- **Pink boxes**: Placeholders you replace with your own values (e.g., `table_name`, `column_name`)

## What You Don't Need to Do

- No Bazel commands
- No local builds
- No manual file editing in `docs/generated/sql/bnf/`
- No pushing to `generated-diagrams` repo

## Validation

CI runs these checks automatically:

| Check | What it catches |
|-------|-----------------|
| Missing BNF files | Spec name not in BUILD.bazel |
| Broken grammar links | References to non-existent rules |
| Naming conflicts | Duplicate `_stmt` suffixes |
| Replace/unlink mismatch | Replaced terms that should be unlinked |
| SKIP DOC suppressions | Grammar rules hidden from docs |

If validation fails, your PR will show the errors.

## SKIP DOC Warnings

Some grammar rules are marked `SKIP DOC` in `sql.y` and won't appear in diagrams. The PR comment shows these in a collapsible section so you know what's being excluded.

## Troubleshooting

### Diagram isn't generating
1. Check that `name` matches an entry in `docs/generated/sql/bnf/BUILD.bazel`
2. Verify the `stmt` production exists in `sql.y`

### Diagram has broken links
Add the broken link target to the `unlink` list.

### Diagram is too complex
Use `inline` to expand sub-productions, or `exclude` to hide branches.

## Resources

- [Railroad Diagram Generator](http://www.bottlecaps.de/rr/ui) - The tool that generates diagrams
- [SQL Grammar Wiki](https://github.com/cockroachdb/docs/wiki/SQL-Grammar-Railroad-Diagram-Changes) - Additional documentation

## Questions?

Contact the Education Infra team.
