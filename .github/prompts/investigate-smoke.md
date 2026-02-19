# Tool Smoke Test

You are running a quick smoke test to verify that the tools available
in this GitHub Action environment work correctly. This is NOT a real
investigation — just a connectivity and permissions check.

Your exact tool permissions are defined in the `--allowedTools`
argument in `.github/workflows/investigate.yml`. Read that file
first to see the full allowlist.

## Instructions

For each tool category below, run one trivial command and record the
result. Then write `artifacts/findings.md` with the results.

```bash
mkdir -p artifacts
```

### Tests to run

1. **Write tool**: Write a short string to `artifacts/test-write.txt`
2. **Bash(mkdir)**: `mkdir -p artifacts/test-dir`
3. **Bash(ls)**: `ls -la artifacts/`
4. **Bash(cat)**: `cat CLAUDE.md | head -5`
5. **Bash(grep)**: `grep -r "func Test" pkg/util/log/ | head -3`
6. **Bash(wc)**: `wc -l CLAUDE.md`
7. **Bash(git log)**: `git log --oneline -3`
7b. **Bash(git history + show)**: Run `git log --oneline -20 pkg/kv/kvserver/replica.go`,
    pick one of the older commits from the list, and run `git show <sha> -- pkg/kv/kvserver/replica.go | head -40`.
    This verifies that the blobless clone can transparently fetch old diffs.
8. **Bash(gh issue view)**: `gh issue view <ISSUE_NUMBER> --json title -q .title`
   (use the issue number from the variables below)
9. **Bash(gh search)**: `gh search issues "test" --repo <REPO> --limit 1 --json number`
10. **Bash(fetch-url)**: `fetch-url "https://teamcity.cockroachdb.com/guestAuth/app/rest/server" /dev/null`
11. **Bash(unzip)**: download any small zip and list it, or just run `unzip -l` on a nonexistent file to confirm the command runs
12. **Read tool**: Read `pkg/BUILD.bazel` (first 10 lines)
13. **Grep tool**: Search for "package" in `pkg/util/log/`
14. **Glob tool**: Find `*.go` files in `pkg/util/log/`
15. **WebFetch tool**: Fetch `https://httpbin.org/get`

## Output

Write `artifacts/findings.md` with a results table:

```markdown
## Smoke Test Results

| # | Tool | Command | Result |
|---|------|---------|--------|
| 1 | Write | Write("artifacts/test-write.txt", ...) | ✅ / ❌ |
| 2 | Bash(mkdir) | mkdir -p artifacts/test-dir | ✅ / ❌ |
...
```

If any tool is denied, note the exact error message. End with a
summary of what works and what doesn't.
