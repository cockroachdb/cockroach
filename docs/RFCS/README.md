# About this directory

This directory contains RFCs (design documents) that describe
proposed changes to cockroach.

# RFC process

Before making major changes, consider sending an RFC for discussion.
This is a lightweight design-document process that is inspired by the
[process used by the rust project](https://github.com/rust-lang/rfcs).

1. Copy `template.md` to a new file and fill in the details. Commit
   this version in your own fork of the repository or a branch.

2. Submit a pull request to add your new file to the main repository.
   Each RFC should get its own pull request; do not combine RFCs with
   other files.

3. Go through the PR review. When the dust on the PR review has settled,
   and if there is consensus to proceed with the project, begin the
   final comment period (FCP) by (1) posting a comment on the PR and
   (2) posting an announcement on the persistent public communication
   channel du jour (https://forum.cockroachlabs.com/ at the time of
   this writing).
   The FCP should last a minimum of two working days to allow
   collaborators to voice any last-minute concerns. If there is still
   consensus to proceed after the FCP, change the `Status` field of the
   document to `in-progress`, update the `RFC PR` field, and merge the
   PR. If the project is rejected, either abandon the PR or merge it
   with a status of `rejected` (depending on whether the document and
   discussion are worth preserving for posterity).
   Note that it is possible for an RFC to receive discussion after it
   has been approved and its PR merged, e.g. during implementation.
   While this is undesirable and should generally be avoided, such
   discussion should still be addressed by the initiator (or
   implementer) of the RFC. For instance, this can happen when an
   expert is not available during the initial review (e.g. because she
   is on vacation).

4. Once the changes described in the RFC have been made, change the
   status of the PR from `in-progress` to `completed`. If subsequent
   developments render an RFC obsolete, set its status to `obsolete`.

When you mark a RFC as obsolete, ensure that its text references the
other RFCs or PRs that make it obsolete.

# RFC Status

During its lifetime an RFC can have the following status:

- Draft

  A newly minted RFC has this status, until either the proposal is
  accepted (next possible status: in-progress) or that it is DOA (next
  possible status: rejected).

- In-progress

  A RFC receives this status when the PR discussions have concluded
  the proposal should be implemented, and the RFC is ready to commit
  (merge) to the main repository.

  Next possible status: completed (when the work is done), rejected
  (proposal not implemented/implementable after all), obsolete (some
  subsequent work removes the need for the feature).

- Completed

  A RFC receives this status when the described feature has been
  implemented. It may stay with this status indefinitely or until made
  obsolete.

- Obsolete

  A RFC receives this status when the described feature has been
  superseded.
