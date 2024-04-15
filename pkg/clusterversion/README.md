Cluster versions
---

# Introduction

TODO(radu): move most of the documentation from clusterversion.go.

# Runbooks

This section contains runbooks for the necessary steps associated with releasing
new versions.

## Overview

The graph below shows the steps that have to be taken to release a new version
and update master for the following version. We use the example of releasing
24.1 (in the line of releases 23.1, 23.2, 24.1, 24.2).

```mermaid
graph TD;
  base("Cut release-24.1 branch
        Latest = 23.2-x
        MinSupported = 23.1
        version.txt = v24.1.0-alpha.00000000")
    
  base -- release-24.1 --> rel1
  
  rel1("R.1: Prepare for beta
        Latest = 23.2-x
        MinSupported = 23.1
        developmentBranch = false
        version.txt = v24.1.0-beta.1")
  
  rel1 --> rel2
  
  rel2("R.2: Mint release
        Latest = 24.1
        MinSupported = 23.1
        developmentBranch = false
        finalVersion = V24_1
        version.txt = v24.1.0")


  base -- master --> master1
  
  master1("M.1: Bump min supported
           Latest = 23.2-x
           MinSupported = 23.2
           version.txt = v24.1.0-alpha.00000000")
            
  master1 --> master2
    
  master2("M.2: Bump current version
           Latest = 24.1-2
           MinSupported = 23.2
           version.txt = v24.2.0-alpha.00000000")
            
  master2 --> master3
            
  master3(M.3: Finalize gates and bootstrap data for released version)
```

## Release branch changes

The changes below happen directly on the release branch (and only on that
branch). They are not normal backports of changes from `master`.

### R.1: Prepare for beta

**What**: This change prepares the branch for releasing the initial beta. The
first beta is special because starting with this version we allow production
clusters from the previous version to be upgraded to the beta version; and,
consequently, we allow and support upgrading from the beta cluster version to
the final release cluster version. This transition is achieved by setting
`developmentBranch` to `false`; see `developmentBranch` and `DevOffset` in the
code for more details.

**When**: When we are ready to select the first beta candidate.

**Checklist**:
 - [ ] Set `developmentBranch` constant to `false`
 - [ ] Update `version.txt` to the beta version, e.g. `24.1.0-beta.1`
 - [ ] Regenerate docs (`./dev gen docs`)
 - [ ] Regenerate expected test data results as needed

**Example PR:** [#113912](https://github.com/cockroachdb/cockroach/pull/113912)

### R.2: Mint release

**What**: This change finalizes the cluster version for the release.

**When**: When we are absolutely sure that we no longer need additional version
gates - right before the final RC at the latest.

**Checklist**:
- [ ] Replace temporary constant for current release (e.g. `V24_1`) with a
  cluster version key, associated with a "final" (`Internal=0`) version (e.g.
  `24.1`)
- [ ] Set `finalVersion` constant to the key (e.g. `V24_1`)
- [ ] Update `pkg/build/version.txt` to the final version (e.g. `24.1.0`)
- [ ] Regenerate docs (`./dev gen docs`)
- [ ] Regenerate expected test data results as needed

**Example PR:** [#112347](https://github.com/cockroachdb/cockroach/pull/112347)

## Master branch changes

The changes below happen on the `master` branch after forking the release, and
are not backported to the `release-xx.y` branch.

### M.1: Bump min supported

**What**: This change advances the `MinSupported` version. This is normally two
versions behind the current release (we support "skipping" a version during
upgrade as an experimental feature). Once `MinSupported` is advanced, all older
non-permanent version keys are no longer necessary - Cockroach code will never
deal with a cluster below `MinSupported`.

**When**: It is recommended to start this process when the first release
candidate is cut (i.e. there are no open GA blockers). The main reason is to
avoid large changes on `master` which might cause merge conflicts for backports.

**Checklist**:

- [ ] Advance `MinVersion` to the previous release (e.g. `V23.2`)

- [ ] Rename all non-permanent version keys below `MinVersion`, prepending
  `TODO_Delete_` to them.
  <details><summary>Additional context</summary>
  The way we name version gates can potentially be misleading:

   - Gates beginning with `{MAJOR_SERIES}*` help migrate the system from
     `{MAJOR_SERIES}-1` to `{MAJOR_SERIES}`. These gates have cluster versions
     **below** that of the final release version for `{MAJOR_SERIES}`: they use the
     previous series major and minor and a non-zero (and even) internal value. For
     example, `V23_2SomeFeature` would be associated with a version like `v23.1-x`
     (e.g. `v23.1-8`).

   - Once `MinSupported` is advanced to `{MAJOR_SERIES}`  we no longer need these
     `{MAJOR_SERIES}*` gates. For example, a gate like `V23_2SomeFeature` is used
     during 23.1 → 23.2 upgrade; once `MinSupported = V23_2` this gate will always be
     "active" in any clusters our code has to deal with.
  </details>

- [ ] Remove logictest config that bootstraps to the old minimum supported
  version (and run `./dev gen testlogic`).

- [ ] File issue(s) to remove `TODO_Delete_` uses and simplify related code; assign
  to relevant teams (depending on which top-level packages use such gates).
  <details><summary>For the overachiever</summary>
  Historically, these cleanup issues have not received much attention and the code
  was carried over for a long time. Feel free to ping individuals or do some of
  the cleanup directly (in separate PRs). The cleanup comes down to simplifying the
  code based on the knowledge that `IsActive()` will always return `true` for
  obsolete gates. If simplifying the code becomes non-trivial, error on the side
  of just removing `IsActive()` calls and leaving TODOs for further cleanup.
  </details>
  
- [ ] Regenerate expected test data results as needed; file issues for any
  skipped tests. Note that upgrade tests in `pkg/upgrade/upgrades` use
  `clusterversion.SkipWhenMinSupportedVersionIsAtLeast()` so they can be removed
  later (as part of cleaning up the obsolete gates).

**Example PRs:** [#121775](https://github.com/cockroachdb/cockroach/pull/121775)
[#122062](https://github.com/cockroachdb/cockroach/pull/122062)
[#122244](https://github.com/cockroachdb/cockroach/pull/122244)

### M.2: Bump current version

**What**: This change advances the current release series version.

**When**: Any time after M.1.

**Checklist**:

- [ ] Add version key constant for new release (e.g. `V24.2`), equal to `Latest`

- [ ] Add start version (e.g. `V24.2Start` with version `24.2-2`)

- [ ] Update `pkg/build/version.txt` to the new version (e.g. `v24.2.0-alpha.00000000`)

- [ ] Add mixed version logictest config for the replaced version (`local-mixed-24.1`)

- [ ] Create new SQL bootstrap data. This is necessary because we want code on
  `master` to be able to bootstrap clusters at the previous version, but we will
  no longer have that code around. The data is obtained from the release branch
  (e.g. `release-24.1`) using the `sql-bootstrap-data` utility:
  ```
  ./dev build sql-bootstrap-data
  ./bin/sql-bootstrap-data
  ```
  This will create a pair of files that need to be copied to
  `pkg/sql/catalog/bootstrap/data` on the `master` branch; it will also output
  what code modifications need to be performed.

- [ ] Update releases file:
  ```
  bazel build //pkg/cmd/release:release
  _bazel/bin/pkg/cmd/release/release_/release update-releases-file
  ```

**Example PR:** [#112271](https://github.com/cockroachdb/cockroach/pull/112271)

### M.3: Finalize gates and bootstrap data for released version

**What**: Bring in any last changes to bootstrap data and version gates from the
release.

**When**: Any time after minting the previous release (R.2), and after M.2.

**Checklist**:

- [ ] Repeat the "create new SQL bootstrap data" step from
  [M.2](#m2-bump-current-version)

- [ ] Create the final gate for the previous release, the same one created in
  [R.2](#r2-mint-release). All gates from the previous release should be identical
  on the `master` and release branches.

**Example PR:** TODO
