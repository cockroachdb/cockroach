---
globs: ["**/*.go"]
---

# Log and Error Redactability

CockroachDB implements redactability to ensure sensitive information (PII,
confidential data) is automatically removed or marked in log messages and error
outputs. This enables customers to safely share logs with support teams.

## Core Concepts

**Safe vs Unsafe Data:**
- **Safe data**: Information certainly known to NOT be PII-laden (node IDs, range IDs, error codes)
- **Unsafe data**: Information potentially PII-laden or confidential (user data, SQL statements, keys)

**Redactable Strings:**
- Unsafe data is enclosed in Unicode markers: `‹unsafe_data›`
- Safe data appears without markers
- Log entries show a special indicator: `⋮` (vertical ellipsis)

## Key Implementation Patterns

**SafeValue Interface** - For types that are always safe:
```go
type NodeID int32
func (n NodeID) SafeValue() {}  // always safe to log

// Interface verification pattern.
var _ redact.SafeValue = NodeID(0)
```

**SafeFormatter Interface** - For complex types mixing safe/unsafe data:
```go
func (s *ComponentStats) SafeFormat(w redact.SafePrinter, _ rune) {
    w.Printf("ComponentStats{ID: %v", s.Component)
    // Use w.Printf(), w.SafeString(), w.SafeRune() to mark safe parts.
}
```

## Common APIs

- `redact.Safe(value)` - Mark a value as safe
- `redact.SafeString(s)` - Mark string literal as safe

## Redactcheck Linter

Prefer using SafeFormatter, which does not require the below check.
If implementing SafeValue instead:

The linter in `/pkg/testutils/lint/passes/redactcheck/redactcheck.go`:
- Maintains allowlist of types permitted to implement `SafeValue`
- Validates `RegisterSafeType` calls
- Prevents accidental marking of sensitive types as safe

To add a new safe type:
1. Implement `SafeValue()` method
2. Add interface verification: `var _ redact.SafeValue = TypeName{}`
3. Update redactcheck allowlist if needed

## Key Files

- `/pkg/util/log/redact.go` - Core redaction logic
- `/docs/RFCS/20200427_log_file_redaction.md` - Design RFC
- `/pkg/testutils/lint/passes/redactcheck/redactcheck.go` - Linter implementation
- `/pkg/util/log/redact_test.go` - Test examples and patterns
