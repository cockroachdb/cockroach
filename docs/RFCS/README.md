# About this directory

This directory contains RFCs (short design documents) that describe
proposed changes to cockroach.

# RFC process

Before making major changes, consider sending an RFC for discussion.
This is a lightweight design-document process that is inspired by the
[process used by the rust project](https://github.com/rust-lang/rfcs).

1. Copy `template.md` to a new file and fill in the details.
2. Submit a pull request to add your new file. Each RFC should get its
   own pull request; do not combine RFCs with other files.
3. Go through the PR review. If there is consensus to proceed with the
   project, change the `Status` field of the document to
   `in-progress`, update the `RFC PR` field, and merge the PR. If the
   project is rejected, either abandon the PR or merge it with a
   status of `rejected` (depending on whether the document and
   discussion are worth preserving for posterity).
4. Once the changes described in the RFC have been made, change the
   status of the PR from `in-progress` to `completed`. If subsequent
   developments render an RFC obsolete, set its status to `obsolete`.
