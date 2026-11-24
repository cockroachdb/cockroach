# M.5: Finalize Gates and Bootstrap Data for Released Version

### Overview

**When to perform:** After the final `.0` release is published (e.g., `v26.1.0`).

**What it does:** Updates the bootstrap data that was generated during M.2 to
reflect the final released version. The M.2 bootstrap data was generated from
the release branch before the final release was cut; M.5 refreshes it from the
exact release tag.

**Dependencies:**
- M.2 must be completed (bootstrap data for the release already exists on master)
- The final `.0` release tag must exist (e.g., `v26.1.0`)

---

### Step-by-Step Checklist

#### Step 1: Confirm prerequisites

```bash
# Verify the final release tag exists
git ls-remote origin 'refs/tags/v26.1.0'

# Confirm bootstrap data from M.2 is already on master
ls pkg/sql/catalog/bootstrap/data/26_1_*
```

#### Step 2: Create a working branch

```bash
git checkout master && git pull
git checkout -b m5-bootstrap-26.1
```

#### Step 3: Create a git worktree for the release tag

Using a worktree avoids switching branches in your main working tree.

```bash
git fetch origin release-26.1
git worktree add /tmp/release-26.1-worktree origin/release-26.1
cd /tmp/release-26.1-worktree
git checkout v26.1.0
```

**Important:** Use the `.0` release tag, not the current HEAD of the release
branch. The HEAD may have backports applied after the release was cut.

#### Step 4: Build and run the bootstrap data tool

```bash
cd /tmp/release-26.1-worktree
./dev doctor   # only needed if first time using this worktree
./dev build sql-bootstrap-data && bin/sql-bootstrap-data
```

The tool generates 4 files in `pkg/sql/catalog/bootstrap/data/`:
- `26_1_system.keys`
- `26_1_system.sha256`
- `26_1_tenant.keys`
- `26_1_tenant.sha256`

#### Step 5: Copy the new files to master

```bash
cp /tmp/release-26.1-worktree/pkg/sql/catalog/bootstrap/data/26_1_* \
   ~/go/src/github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap/data/
```

#### Step 6: Verify SHA256 hashes

```bash
cd ~/go/src/github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap/data

sha256sum 26_1_system.keys | awk '{print $1}' | tr -d '\n' > /tmp/actual.sha256
diff /tmp/actual.sha256 26_1_system.sha256 && echo "system hash OK"

sha256sum 26_1_tenant.keys | awk '{print $1}' | tr -d '\n' > /tmp/actual.sha256
diff /tmp/actual.sha256 26_1_tenant.sha256 && echo "tenant hash OK"
```

If the hashes didn't change, the bootstrap data was already up to date and
no commit is needed.

#### Step 7: Check version gate parity

Verify all V26_1_* gates have the same version values on both branches:

```bash
# Extract gates from master
grep "V26_1" pkg/clusterversion/cockroach_versions.go \
  | grep -v "PreviousRelease\|finalVersion\|V26_2" > /tmp/master_gates.txt

# Extract gates from release
grep "V26_1" /tmp/release-26.1-worktree/pkg/clusterversion/cockroach_versions.go \
  | grep -v "PreviousRelease\|finalVersion" > /tmp/release_gates.txt

diff /tmp/master_gates.txt /tmp/release_gates.txt && echo "Gates match"
```

If there are differences, investigate whether any V26_1 gates were added to
the release branch but not backported to master (or vice versa). File issues
as needed.

#### Step 8: Run tests

```bash
cd ~/go/src/github.com/cockroachdb/cockroach
./dev test pkg/sql/catalog/bootstrap -v
```

All tests should pass.

#### Step 9: Clean up the worktree

```bash
git worktree remove --force /tmp/release-26.1-worktree
```

---

### Expected Files Modified

Only bootstrap data files change — no Go code changes are needed:

1. `pkg/sql/catalog/bootstrap/data/26_1_system.keys`
2. `pkg/sql/catalog/bootstrap/data/26_1_system.sha256`
3. `pkg/sql/catalog/bootstrap/data/26_1_tenant.keys`
4. `pkg/sql/catalog/bootstrap/data/26_1_tenant.sha256`

If the hashes are unchanged from M.2, the release was cut without further
schema changes and there is nothing to commit.

---

### Validation Checklist

- [ ] Release tag (e.g., `v26.1.0`) exists on origin
- [ ] Bootstrap data generated from the exact `.0` tag (not branch HEAD)
- [ ] SHA256 hashes verified for both system and tenant files
- [ ] Hashes differ from the M.2-generated data (if same, no PR needed)
- [ ] Version gate parity confirmed between master and release branch
- [ ] Bootstrap tests pass

---

### Common Errors

**"working trees containing submodules cannot be moved or removed"**

Use `--force` when removing the worktree:
```bash
git worktree remove --force /tmp/release-26.1-worktree
```

**Hashes are the same as M.2**

If the `.sha256` files don't change, the schema was frozen before the final
release and M.5 is a no-op. No PR needed.

**Gate diff shows differences**

A gate present on the release branch but missing from master means it was
added as a backport after M.2/M.3 were merged. File a bug and add the gate
to master manually (following the same versioning as on the release branch).

---

### Quick Reference

```bash
# Create worktree at the .0 tag
git fetch origin release-26.1
git worktree add /tmp/release-26.1-worktree origin/release-26.1
cd /tmp/release-26.1-worktree && git checkout v26.1.0

# Generate bootstrap data
./dev build sql-bootstrap-data && bin/sql-bootstrap-data

# Copy to master
cp /tmp/release-26.1-worktree/pkg/sql/catalog/bootstrap/data/26_1_* \
   ~/go/src/github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap/data/

# Verify hashes
cd ~/go/src/github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap/data
sha256sum 26_1_system.keys | awk '{print $1}' | tr -d '\n' > /tmp/a && diff /tmp/a 26_1_system.sha256 && echo OK
sha256sum 26_1_tenant.keys | awk '{print $1}' | tr -d '\n' > /tmp/a && diff /tmp/a 26_1_tenant.sha256 && echo OK

# Run tests
cd ~/go/src/github.com/cockroachdb/cockroach && ./dev test pkg/sql/catalog/bootstrap -v

# Clean up
git worktree remove --force /tmp/release-26.1-worktree
```

**Example PR:** #165680
