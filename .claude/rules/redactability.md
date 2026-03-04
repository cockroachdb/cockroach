---
globs: ["**/*.go"]
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