# About this directory

This directory contains RFCs (design documents) that describe
proposed changes to cockroach.

# Why and when to send a RFC?

Before making major changes, consider sending an RFC for discussion.
This is a lightweight design-document process that is inspired by the
[process used by the rust project](https://github.com/rust-lang/rfcs).

What is a major change?

A change is "major" and requires a RFC when any of the following
conditions apply:

- completing the change will cost you more than a week of work.
- reviewers from more than one team are needed to review the change.
- users will need to change the way they use CockroachDB as a result.
- other CockroachDB contributors will need to change the way
  they work as a result.
- the architecture of the software or the project changes in a way visible
  to more than a handful of developers.
- the change impacts the cost model of CockroachDB (how much time and
  how much memory is used for common operations) in a way that's
  noticeable by users.

# RFC process

1. Copy `00000000_template.md` to a new file and fill in the details. Commit
   this version in your own fork of the repository or a branch.

2. Submit a pull request (PR) to add your new file to the main repository.
   Each RFC should get its own pull request; do not combine RFCs with
   other files.

   Note: you can submit a PR before the RFC is complete, to solicit
   input about what to write in the RFC. In this case, include the word
   "WIP" in the PR title and use the label do-not-merge, until
   you are confident the RFC is complete and can be reviewed.

   **Before you consider the RFC ready to be reviewed, give extra
   attention to the section "Rationale and Alternatives". It's
   important for the reviewers to understand why the solution chosen
   was right. It is an unfortunate fact that often a solution is
   determined first and the RFC is then written to support that
   solution, without much consideration being given to the
   alternatives. You should be mindful of this when it happens and
   attempt to compensate by a serious review of alternatives.**

3. Go through the PR review, iterating on the RFC to answer questions
   and concerns from the reviewer(s). This process can last anywhere
   from a couple of days to several weeks, depending on the complexity
   of the proposal.

   If the scope of the RFC is
   [large](https://www.cockroachlabs.com/docs/stable/contribute-to-cockroachdb.html),
   there should be at least two designated reviewers from the RFC's
   expertise area.

4. At some point, the author(s) and/or review coordinator will seek to
   decide that the "dust has settled".

   The author(s) and/or review coordinator can move this process along
   by pushing to conclude open discussions one way or another. This
   includes, but is not limited to:

   - answering unanswered questions themselves,
   - seeking participation from other engineers to answer open questions,
   - reducing the scope of the RFC so that open questions become out of scope,
   - postponing a decision or answer to a later implementation phase,
   - organize a plenary session where open concerns are discussed orally,
   - postponing/cancelling the work altogether.

   In either case, the process to push for conclusion should seek
   approval to conclude from the reviewer(s) who have opened the
   discussions so far, allowing sufficient time for them to evaluate
   the proposed conclusion.

5. At the point where the author(s) and/or review coordinator
   estimate there is consensus to proceed with the project, begin the
   final comment period (FCP) by (1) posting a comment on the PR and
   (2) posting an announcement on the persistent public communication
   channel du jour (https://forum.cockroachlabs.com/ at the time of
   this writing).  The FCP should last a minimum of two working days
   to allow collaborators to voice any last-minute concerns.

6. If there is still consensus to proceed after the FCP:

   - change the `Status` field of the document to `in-progress`;
   - rename the RFC document to prefix it with the current date (`YYYYMMDD_`);
   - update the `RFC PR` field;
   - and merge the PR.

   If the project is rejected, either abandon the PR or merge it
   with a status of `rejected` (depending on whether the document and
   discussion are worth preserving for posterity). The project may
   receive a status of `postponed` rather than `rejected` if
   it is likely to be implemented in the future.

   Note that it is possible for an RFC to receive discussion after it
   has been approved and its PR merged, e.g. during implementation.
   While this is undesirable and should generally be avoided, such
   discussion should still be addressed by the initiator (or
   implementer) of the RFC. For instance, this can happen when an
   expert is not available during the initial review (e.g. because she
   is on vacation).

7. Once the changes described in the RFC have been made, change the
   status of the PR from `in-progress` to `completed`. If subsequent
   developments render an RFC obsolete, set its status to `obsolete`.

When you mark a RFC as obsolete, ensure that its text references the
other RFCs or PRs that make it obsolete.

# Rationale about minimum delays and process complexity

There is a trade-off in every organisation between the impetus to make
more time for decision-making and consensus-reaching, mandating
minimum wait times to let every participants evaluate the work, and
the impetus to move fast and sometimes discover/fix problems later.

At the time of this writing (2017), Cockroach Labs is biased towards
the second end of this spectrum, and this RFC process correspondingly
does not spell out minimum wait delays in the RFC process other than
the 1-2 working day FCP.

# RFC Status

During its lifetime an RFC can have the following status:

- Draft

  A newly minted RFC has this status, until either the proposal is
  accepted (next possible status: in-progress), deferred (next possible
  status: postponed) or that it is DOA (next possible status: rejected).

- Rejected

  A RFC receives this status when the PR discussions have concluded
  the proposal should not be implemented.

  Next possible status: none. If you want to ressucitate an idea, make
  a new RFC.

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

- Postponed

  A RFC receives this status when the PR discussions have concluded that
  due to implementation complexity, lack of customer demand, or other
  issues with the proposal, the work should be postponed to a later date.

  Next possible status: draft (the decision has been made to reconsider
  this proposal), rejected (proposal not implemented/implementable after all),
  obsolete (some subsequent work removes the need for the feature).
