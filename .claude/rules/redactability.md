---
paths:
  - "**/*.go"
---

# Redactability: types in logs and errors

Not every type needs a `String()` method — but if a type does implement
`fmt.Stringer` or the `error` interface, its output must be redactable.
Unredacted strings in logs leak PII in customer-shared diagnostics.

**When to act:** adding a `String()` method, implementing `error`, or
defining a type that will appear in log or error output.

**Decision guide:**
- Type contains only IDs, counts, or other non-PII → implement `SafeValue`:
  ```go
  func (t MyType) SafeValue() {}
  var _ redact.SafeValue = MyType{}
  ```
- Type mixes safe and unsafe fields → implement `SafeFormatter`:
  ```go
  func (t *MyType) SafeFormat(w redact.SafePrinter, _ rune) {
      w.Printf("MyType{id: %v, ", redact.Safe(t.ID))
      w.Printf("name: %v}", t.Name) // unsafe by default
  }
  // String implements fmt.Stringer via SafeFormat.
  func (t *MyType) String() string {
      return redact.StringWithoutMarkers(t)
  }
  ```
- If implementing `SafeValue`, update the allowlist in
  `pkg/testutils/lint/passes/redactcheck/redactcheck.go`.

Prefer `SafeFormatter` over `SafeValue` — it's more precise and doesn't
require linter allowlist changes.

**Hash-based redaction:** CockroachDB supports hash-based redaction via
`cockroachdb/redact`. Instead of replacing sensitive values with `×`,
hashing produces a deterministic truncated hash (e.g. `‹2bd806c9›`),
allowing correlation across log entries without exposing raw PII.

- Wrap values with the appropriate `Hash*` type for hash-based redaction.
  When hashing is enabled, the value is replaced by its hash; when
  disabled, it falls back to full redaction (`×`). Choose the type that
  matches your value:
  | Value type       | Hash wrapper          |
  |------------------|-----------------------|
  | `string`         | `redact.HashString`   |
  | `int`            | `redact.HashInt`      |
  | `uint`           | `redact.HashUint`     |
  | `float64`        | `redact.HashFloat`    |
  | `[]byte`         | `redact.HashBytes`    |
  | `byte`           | `redact.HashByte`     |
  | `rune`           | `redact.HashRune`     |
- For custom types, implement the `redact.HashValue` marker interface so
  the redact package hashes them instead of replacing with `×`. Types implementing this interface should alias a base Go type — the hash is computed on the value's string representation during `Redact()`:
  ```go
  type UserID string
  func (UserID) HashValue() {}
  var _ redact.HashValue = UserID("")
  ```
- Hashing is controlled globally via `redact.EnableHashing(salt)` /
  `redact.DisableHashing()`. In CockroachDB, this is configured through
  the log config (`redaction.hashing.enabled` / `redaction.hashing.salt`
  in `pkg/util/log/logconfig/config.go`).
- Always provide a salt in production to resist brute-force reversal.
- Regular unsafe values (not wrapped with a `Hash*` type or `HashValue`)
  are still fully redacted even when hashing is enabled — hashing is
  opt-in per value.

**When to use hash-based redaction:** use it for values where you need to
correlate occurrences across log lines (e.g. usernames, session IDs) but
must not expose the raw value. Do not use it for values that should never
appear in any form (e.g. passwords, tokens).

**Testing:** nontrivial `SafeFormat` implementations need an `echotest`
unit test to lock down both the redacted and unredacted output:
```go
func TestMyType_SafeFormat(t *testing.T) {
    v := MyType{ID: 1, Name: "sensitive"}
    redacted := string(redact.Sprint(v))
    echotest.Require(t, redacted, datapathutils.TestDataPath(t, t.Name()))
}
```
Run with `-rewrite` to generate the initial testdata file, then inspect
it to verify markers appear around unsafe fields.

For hash-based redaction, verify both hashing-enabled and hashing-disabled
outputs:
```go
func TestMyType_HashRedaction(t *testing.T) {
    redact.DisableHashing()
    t.Cleanup(redact.DisableHashing)

    hashAlice := func() string {
        return string(redact.Sprintf("user=%s", redact.HashString("alice")).Redact())
    }

    t.Run("hashing enabled", func(t *testing.T) {
        redact.EnableHashing(nil)
        defer redact.DisableHashing()
        require.Equal(t, "user=‹2bd806c9›", hashAlice())
    })

    t.Run("hashing disabled", func(t *testing.T) {
        require.Equal(t, "user=‹×›", hashAlice())
    })
}
```