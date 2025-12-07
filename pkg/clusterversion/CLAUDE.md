# CockroachDB Release Preparation Guide

This document provides an overview of the quarterly release preparation tasks with links to detailed runbooks.

## Quick Navigation

**Release Branch Tasks:**
- [R.1: Prepare for beta](runbooks/R1_prepare_for_beta.md) - Prepare the release branch for beta
- [R.2: Mint release](runbooks/R2_mint_release.md) - Finalize cluster version before RC

**Master Branch Tasks:**
- [M.1: Bump Current Version](runbooks/M1_bump_current_version.md) - Advance master to next development version
- [M.2: Enable Mixed-Cluster Logic Tests](runbooks/M2_enable_mixed_cluster_logic_tests.md) - Add bootstrap data and test configs
- [M.3: Enable Upgrade Tests](runbooks/M3_enable_upgrade_tests.md) - Generate fixtures and enable roachtests
  - [M.3 Quick Reference](runbooks/M3_enable_upgrade_tests_QUICK.md) - Streamlined checklist version
- [M.4: Bump MinSupported Version](runbooks/M4_bump_minsupported_version.md) - Remove support for oldest version
  - [M.4 Quick Reference](runbooks/M4_bump_minsupported_version_QUICK.md) - Streamlined checklist version

## For Claude Code AI

**IMPORTANT FOR FUTURE CLAUDE SESSIONS:**

When the user asks you to perform a release task (e.g., "perform M.1", "do the M.2 task", "help with R.1"):

1. **Read the appropriate runbook** from the `runbooks/` directory using the Read tool
   - **Prefer QUICK versions when available** (e.g., `M4_bump_minsupported_version_QUICK.md`)
   - Quick versions are streamlined checklists optimized for execution
   - Full versions have detailed explanations and troubleshooting
2. **Follow the runbook exactly** - it contains step-by-step instructions, expected files, and common errors
3. **Use the TodoWrite tool** to track your progress through the runbook steps
4. **Ask clarifying questions** if the runbook is unclear or doesn't match the current codebase state
5. **Reference the full runbook** when you encounter errors or need deeper explanation

**Adding new runbooks:**

1. **Create a new file** in `pkg/clusterversion/runbooks/` following the naming pattern: `{R|M}N_descriptive_name.md`
2. **Consider creating a QUICK version** for complex tasks (e.g., `{R|M}N_descriptive_name_QUICK.md`)
   - Quick versions should be action-oriented checklists
   - Use tables, bullet points, and minimal prose
   - Focus on WHAT to do, not WHY
3. **Update this file** to add the new runbook to the Quick Navigation section
4. **Update `pkg/clusterversion/README.md`** with a "Claude Prompt" section that references the new runbook
5. **Follow the existing structure:** Use consistent header levels (# for title, ## for major sections, ### for subsections)
6. **Include these sections:** Overview, Prerequisites, Step-by-Step Checklist, Expected Files Modified, Validation/Verification, Common Errors, Quick Reference Commands

## Runbook Structure Guidelines

Each runbook should include:

- **Overview**: When to perform the task, what it does, why it's needed
- **Prerequisites**: What must be completed before starting
- **Step-by-Step Checklist**: Detailed instructions with commands
- **Expected Files Modified**: List of files that should change
- **Validation/Verification**: How to verify the changes are correct
- **Common Errors and Solutions**: Known issues and fixes
- **Quick Reference Commands**: Summary of key commands

## General Tips

### Git Workflow

Always check which branch you're on:
```bash
git status
git log --oneline -5
```

When creating commits, follow the project's commit message format (see root CLAUDE.md).

### Testing

After making changes, always run relevant tests:
```bash
./dev test pkg/clusterversion pkg/roachpb  # Version-related tests
./dev test pkg/sql/catalog/bootstrap       # Bootstrap tests
./dev testlogic                            # Logic tests
```

### Build Verification

Verify the build works after changes:
```bash
./dev build short  # Fast build without UI
```

### Common Patterns

**Version Constants:**
- Format: `V{MAJOR}_{MINOR}_{FeatureName}` for internal versions
- Format: `V{MAJOR}_{MINOR}` for release versions
- Example: `V25_4_Start`, `V25_4_AddNewTable`, `V25_4`

**Internal Version Numbers:**
- Must be even (2, 4, 6, ...)
- Increment by 2 for each new internal version

**File Patterns to Watch:**
- Bootstrap data: `pkg/sql/catalog/bootstrap/data/{version}_*.{keys,sha256}`
- Schema changer rules: `pkg/sql/schemachanger/scplan/internal/rules/release_{version}/`
- Logic test configs: `pkg/sql/logictest/logictestbase/logictestbase.go`

### Multi-Version Pattern

CockroachDB maintains a rolling window of N-2 versions:
- Current development: 26.1 (on master)
- Previous release: 25.4 (MinSupported after M.4)
- Older supported: 25.3 (removed in M.4)

### Quarterly Cycle

1. **Around beta time**: M.1 (bump master version)
2. **After RC published**: M.2 (enable mixed-cluster tests), M.3 (enable upgrade tests)
3. **After final release**: M.4 (bump MinSupported, clean up old versions)
4. **Before final release**: R.1 (prepare beta), R.2 (mint release)

### Validation Strategy

Before creating a PR:
1. Compare file changes with previous PR for the same task
2. Run core tests (clusterversion, storage, bootstrap)
3. Verify build succeeds
4. Check commit message follows conventions
5. Review all modified files for unintended changes
