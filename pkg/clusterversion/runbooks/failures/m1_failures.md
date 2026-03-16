# M.1 Known CI Failure Patterns

Failures encountered during M.1 (Bump Current Version) CI runs. All are Type 2
(testdata updates only, no production code changes).

## Type 2A — Version string in logictest output

**Symptom:**
```
expected: 26.2
got:      26.3
```
or similar version number mismatch in logictest output.

**Cause:** Testdata files reference the old release version string in expected output.

**Fix:**
```bash
# Find affected files
grep -rn "26\.2" pkg/sql/logictest/testdata/ pkg/ccl/logictestccl/testdata/

# Update (replace OLD with the previous release, e.g. 26.2)
sed -i '' 's/26\.2/26\.3/g' <files>
```

Common files: `crdb_internal`, `crdb_internal_catalog`, `crdb_internal_tenant`, `upgrade`.

---

## Type 2B — systemDatabaseSchemaVersion JSON mismatch

**Symptom:**
```
expected: {"internal": 26, "majorVal": 1000026, "minorVal": 1}
got:      {"internal": 2,  "majorVal": 1000026, "minorVal": 2}
```

**Cause:** `crdb_internal_catalog` testdata has the old `V<X>_Start` version triple.
The values come from `SystemDatabaseSchemaBootstrapVersion` which was updated to
point to the new `_Start` version.

**Fix:** Update manually in:
`pkg/sql/logictest/testdata/logic_test/crdb_internal_catalog`

Formula: `majorVal = 1000000 + Major`, `minorVal = Minor`, `internal = Internal`
from the new `_Start` version's entry in `versionTable`.

---

## Type 2C — Bootstrap hash mismatch

**Symptom:**
```
Unexpected hash value for system table bootstrap data.
Expected: <old hash>
Got:      <new hash>
```

**Cause:** `SystemDatabaseSchemaBootstrapVersion` was bumped, changing the bootstrap hash.

**Fix:**
```bash
# Step 1: Update hashes in testdata (copy from the error output)
# Edit: pkg/sql/catalog/bootstrap/testdata/testdata

# Step 2: Rewrite output blocks
./dev test pkg/sql/catalog/bootstrap -f TestInitialValuesToString --rewrite

# Step 3: Rewrite systemschema testdata
bazel test //pkg/sql/catalog/systemschema_test:systemschema_test_test \
  --test_arg=-rewrite \
  --sandbox_writable_path=$(pwd)/pkg/sql/catalog/systemschema_test
```

---

## Type 2H — Issue URL version string in error hints

**Symptom:**
```
expected: ...See: https://go.crdb.dev/issue-v/35730/v26.2
got:      ...See: https://go.crdb.dev/issue-v/35730/dev
```

**Cause:** After bumping `version.txt` to `vX.Y.0-alpha.00000000`, the binary emits
`dev` in error hint URLs. Testdata still has the old pinned version.

**Fix:**
```bash
# Find all occurrences (replace OLD_VER with the previous release, e.g. v26.2)
grep -rn "go.crdb.dev/issue-v/[0-9]*/OLD_VER" \
  pkg/sql/logictest/testdata/ \
  pkg/ccl/logictestccl/testdata/

# Fix (replace OLD_VER with the previous release)
sed -i '' 's|go.crdb.dev/issue-v/\([0-9]*\)/OLD_VER|go.crdb.dev/issue-v/\1/dev|g' <files>
```

Common files: `create_table`, `notice`, and any file with unimplemented error hints.

---

## Type 2D — Declarative rules deprules mismatch

**Symptom:** `deprules` testdata diff in `pkg/sql/schemachanger/scplan/internal/rules/`.

**Cause:** Copying the `current/` rules to a new `release_X_Y/` package changes rule
names (via `rulesVersion` constant), invalidating the deprules golden file.

**Fix:**
```bash
./dev test pkg/cli -f DeclarativeRules --rewrite
```

---

## Notes

- All M.1 failures are Type 2. If you see a Type 1 (production code change needed),
  double-check that you're on the right branch and haven't accidentally modified
  non-testdata files.
- The `check_generated_code` failure from `upgradeinterlockccl` is a local env issue,
  not a real CI failure. See MEMORY.md for the fix.
