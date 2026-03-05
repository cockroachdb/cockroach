# Backporting a change to a release branch

All development starts on the `master` branch.

In most cases, to add a commit to a patch release (or a beta/rc after a release branch has been created), you should submit a PR to `master` first and get it reviewed and merged there.

PRs should be eligible for backport+Policy?search_id=c8caa49f-8458-41b8-8eec-607ce6cba6fc) if you would like to backport them.

Tag your PR with one or more of the `backport` labels (like `backport-21.1.x`, for the 21.1 release) to indicate that it should be backported once merged on master.

## “Must check” for reviewers of backport PRs

* Double check the PR is eligible for backport as per the backport policy+Policy?search_id=c8caa49f-8458-41b8-8eec-607ce6cba6fc).
* Verify that the backport PR **does not add a cluster version nor a version gate**.  
  (i.e. it does not make changes to `pkg/clusterversion/cockroach_versions.go` nor does it contain checks that compare the current version to one of the newer versions.)  
  Ask on #engineering if you need details about why backports must never add cluster versions or version gates.
* Check the release notes in the commit messages, to ensure they are applicable to the branch (if a release note refers to another change / PR that is not being backported, or that may be backported later, suggest a rephrasing.)

## Blathers

Blathers is a bot deployed to cockroachdb/cockroach and is used to facilitate the backport process. Before using Blathers to create backports one must authenticate with Blathers to allow it to access your cockroachdb/cockroach fork.

1. Clone <https://github.com/cockroachlabs/blathers-bot> locally on your laptop.
2. Run `` `go run ./serv/main.go --auth` `` to initiate the authentication flow and follow the instructions. They will ask you to go to `https://github.com/login/device` and enter the device code printed out on the command line. Enter the code and verify the script returns with `` `Successfully saved authentication token ``. If it fails or hangs, retry, if it still does not work ask #dev-inf for help.
3. Leave the following comment`` `blathers auth-check` `` to verify authentication worked. Blathers will respond as you in another comment to say `` `You shall pass` ``. If it responds as itself with `` `You shall not pass` ``try step 2 above again or ask #dev-inf for help.
4. Navigate to <https://github.com/apps/blathers-crl/installations/select_target> and add your fork to the list of repositories Blathers can access. This URL will start with an account picker, please make sure to select your username to find you fork.

Now you are ready to use Blathers for backports. There are two ways to do this via labels or comments.

### Labels

Upon merge, [Blathers](https://github.com/cockroachlabs/blathers-bot) will automatically attempt to create backport PRs for each branch that was indicated via a `backport` label on your PR, i.e. `backport-25.1`. Optionally you can use `backport-all`label to have Blathers automatically backport to all supported release branches.

If any of the backport PRs fail, Blathers will post a comment on the original PR with details and add a `backport-failed` label to the original PR.

Automatic backport can fail for a few reasons, such as:

* backport branch already exists for a PR
* backport has merge conflicts
* backport branch contains merge commits, which are not allowed in backports

If this happens, you must use the manual method (the `backport` command-line tool) described below.

### Comments

You can ask Blathers to create a backport PR without using the tagging system and waiting for merge by commenting on a PR, with the following syntax:

`blathers backport <branch1> <branch2> …`

For example, to backport a PR to 21.1 and 20.2, you would add a comment on the PR that says:

`blathers backport 21.1 20.2`

Blathers will then immediately carry out the backports just as it would when it detects a merged PR with backport tags.

Blathers additionally supports `blathers backport all` command to backport to all supported branches.

## Manual Method with the backport CLI tool

If Blathers was unable to automatically create a backport PR, follow these steps after the main PR is merged to cherry-pick it into the release branch.

1. Make sure the `backport` tool is installed: `go install github.com/cockroachdb/backport@latest`

   1. For first time setup, you will need to set the `cockroach.remote` git config option. Run `git config cockroach.remote <myfork>` where `<myfork>` is the remote in which your fork of cockroach is, e.g. `origin`. You might also need to add `$GOPATH/bin` to your `$PATH`.
2. Run `backport -r VV.V xxxxx`, where `VV.V` is replaced by the version number of the release branch and `xxxxx` is replaced by the GitHub PR number of the PR you are trying to backport, or a space-separated list of PR numbers.  
   This will automatically create a backport branch and upload it to your repository against the release branch to GitHub.   
   Then, it will try to open your browser to create a new PR with that branch, or provide instruction on how to do this manually if it cannot open your browser.

   1. If there were merge conflicts, `backport` will halt and ask you to fix them. Once they're fixed, run `backport --continue` to continue the procedure.
   2. If there were any non-trivial merge conflicts, be sure to call those out in the PR message so reviewers can pay closer attention to the diff.
   3. If something goes wrong, you can use `backport --abort` to give up on the current manual backport.

Note: **You do not need to use bors** to merge a backport PR; just hit the big green button once the CI is green and the backport is approved by the reviewer (usually the main reviewer of the original PR to `master` branch). But note that if the backport has been open and unmerged for many days, CI results could be stale, and might not reflect the current state of the release branch. If the backport has been open and unmerged for many days, be sure to **rebase the backport PR on the tip of the release branch before merging** to get an up-to-date CI run.
