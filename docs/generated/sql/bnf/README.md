# SQL Diagram Generation

This directory contains the BNF (Backus-Naur Form) grammar files and generated SVG railroad diagrams for CockroachDB's SQL syntax.

## For Writers: Adding or Modifying SQL Diagrams

If you need to add a new SQL diagram or modify an existing one, follow these steps:

### Step 1: Edit `diagrams.go`

All diagram definitions are in a single file:
```
pkg/cmd/docgen/diagrams.go
```

Each diagram is defined as a `stmtSpec` struct with the following fields:

| Field | Description |
|-------|-------------|
| `name` | The name of the diagram (must be unique) |
| `stmt` | The grammar production to use (defaults to `name` if empty) |
| `inline` | List of productions to inline/expand in the diagram |
| `replace` | Map of text replacements to apply |
| `regreplace` | Map of regex replacements to apply |
| `match` | Regex patterns that the production must match |
| `exclude` | Regex patterns to exclude from the diagram |
| `unlink` | List of tokens to unlink from the grammar reference |
| `relink` | Map of tokens to relink to different grammar references |
| `nosplit` | If true, don't split the production into multiple lines |

### Step 2: Open a Pull Request

1. Create a branch and edit `pkg/cmd/docgen/diagrams.go`
2. Open a PR against `master`
3. Add the label `docgen-diagram` to your PR

The CI will automatically:
- Generate the BNF files from `sql.y`
- Build the SVG diagrams
- Run validation tests
- Create a PR in `cockroachdb/generated-diagrams` (if changes are detected)

### Step 3: Add `remote_include` (New Diagrams Only)

For **new** diagrams only, you need to add a `remote_include` directive in the docs:

```markdown
{% remote_include https://raw.githubusercontent.com/cockroachdb/generated-diagrams/master/grammar_svg/your_diagram_name.html %}
```

Existing diagrams already have their `remote_include` directives in place.

## What You DON'T Need to Do

- **No Bazel commands** - The CI handles all generation
- **No `./dev generate bnf`** - Automated by CI
- **No `./dev build docs/generated/sql/bnf:svg`** - Automated by CI
- **No manual file editing** in `docs/generated/sql/bnf/`
- **No pushing to generated-diagrams** - Bot handles this

## Example: Adding a New Diagram

```go
// In pkg/cmd/docgen/diagrams.go, add to the specs slice:
{
    name:    "my_new_statement",
    stmt:    "my_new_stmt",  // The production name in sql.y
    inline:  []string{"opt_clause", "another_clause"},
    replace: map[string]string{
        "complex_expr": "simple_name",
    },
    unlink:  []string{"simple_name"},
},
```

Then update `docs/generated/sql/bnf/BUILD.bazel` to add your new diagram name to the `FILES` list.

## Validation

The CI runs several validation checks:

1. **Missing BNF Files** - All specs must have generated BNF files
2. **Missing HTML Diagrams** - All BNF files must have generated HTML
3. **Naming Conflicts** - No `_stmt` suffix conflicts
4. **Replace/Unlink Mismatches** - Replaced tokens should usually be unlinked
5. **Broken Grammar Links** - All diagram links must be valid
6. **SKIP DOC Suppressions** - Reports rules hidden from docs

## Troubleshooting

### My diagram isn't generating

1. Check that the `name` in your spec matches the entry in `BUILD.bazel`
2. Verify the `stmt` production exists in `sql.y`
3. Check the CI logs for validation errors

### My diagram has broken links

Add the broken link target to the `unlink` list in your spec.

### My diagram is too complex

Use the `inline` field to expand sub-productions, or use `match`/`exclude` to filter specific branches.

## Resources

- [SQL Grammar Wiki](https://github.com/cockroachdb/docs/wiki/SQL-Grammar-Railroad-Diagram-Changes)
- [Railroad Diagram Generator](http://www.bottlecaps.de/rr/ui)
