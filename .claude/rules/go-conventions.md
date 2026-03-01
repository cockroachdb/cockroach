---
globs: ["**/*.go"]
---

# CockroachDB Go Conventions

These are CockroachDB-specific Go conventions beyond standard Go style.

## Bool Parameters

Avoid naked bools - use comments or custom types:
```go
printInfo("foo", true /* isLocal */, true /* done */)

// Better: custom types.
type EndTxnAction bool
const (
    Commit EndTxnAction = false
    Abort  = true
)
func endTxn(action EndTxnAction) {}
```

## Enums

Start enums at one unless zero value is meaningful:
```go
type Operation int
const (
    Add Operation = iota + 1
    Subtract
    Multiply
)
```

## Error Flow

Reduce nesting - handle errors early:
```go
for _, v := range data {
    if v.F1 != 1 {
        log.Printf("Invalid v: %v", v)
        continue
    }
    v = process(v)
    if err := v.Call(); err != nil {
        return err
    }
    v.Send()
}
```

Reduce variable scope:
```go
if err := f.Close(); err != nil {
    return err
}
```

## Mutexes

Embed mutex in struct for private types; use a named `mu` field for exported types:
```go
// Private type: embed directly.
type smap struct {
    sync.Mutex
    data map[string]string
}

// Exported type: named field.
type SMap struct {
    mu   sync.Mutex
    data map[string]string
}
```

## Channels

Channel size should be one or unbuffered (zero). Any other size requires
high scrutiny and justification.

## Slice and Map Ownership

Comments should clarify whether a function captures or copies its slice/map arguments:
```go
// SetTrips sets the driver's trips.
// Note that the slice is captured by reference, the
// caller should take care of preventing unwanted aliasing.
func (d *Driver) SetTrips(trips []Trip) { d.trips = trips }
```

## Empty Slices

Prefer `var` declaration for empty slices (not `[]int{}`). `nil` is a valid
empty slice. Check emptiness with `len(s) == 0`, not `s == nil`.

## Defer for Cleanup

Always use `defer` for releasing locks, closing files, and similar cleanup.

## Struct Initialization

Always specify field names. Use `&T{}` instead of `new(T)`.
```go
k := User{
    FirstName: "John",
    LastName: "Doe",
}
sptr := &T{Name: "bar"}
```

## String Performance

Prefer `strconv` over `fmt` for primitive conversions. Avoid repeated
`[]byte("...")` conversions in loops; convert once and reuse.

## Type Assertions

Always use the "comma ok" idiom:
```go
t, ok := i.(string)
if !ok {
    return errors.New("expected string type")
}
```

## Functional Options

Use the `Option` interface pattern for extensible function configuration:
```go
type Option interface {
    apply(*options)
}

func WithTimeout(t time.Duration) Option {
    return optionFunc(func(o *options) {
        o.timeout = t
    })
}

func Connect(addr string, opts ...Option) (*Connection, error) { /* ... */ }
```
