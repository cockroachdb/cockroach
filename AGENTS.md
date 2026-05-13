# CockroachDB Agent Guidelines

This file configures AI coding agents working in the CockroachDB repository.
Follow these conventions when performing automated code reviews.

## Review Initialization

At the start of each review session, establish a timestamp by posting:
```bash
gh pr comment $PR_NUMBER --body "**CockroachDB automated review** — $(date -u '+%Y-%m-%dT%H:%M:%SZ') — commit $(gh pr view $PR_NUMBER --json headRefOid --jq '.headRefOid[:7]')"
```
This creates an audit entry for the review run.

## Code Review Standards

- Focus on correctness, performance, and Go best practices
- Flag potential race conditions, especially in distributed systems code
- Check for proper error handling following CockroachDB conventions
- Verify that new SQL features respect existing semantics
- Inline comments for specific line issues, top-level summary comment for overall assessment
