# circuitbreaker

Circuitbreaker provides an easy way to use the Circuit Breaker pattern in a
Go program.

Circuit breakers are typically used when your program makes remote calls.
Remote calls can often hang for a while before they time out. If your
application makes a lot of these requests, many resources can be tied
up waiting for these time outs to occur. A circuit breaker wraps these
remote calls and will trip after a defined amount of failures or time outs
occur. When a circuit breaker is tripped any future calls will avoid making
the remote call and return an error to the caller. In the meantime, the
circuit breaker will periodically allow some calls to be tried again and
will close the circuit if those are successful.

You can read more about this pattern and how it's used at:
- [Martin Fowler's bliki](http://martinfowler.com/bliki/CircuitBreaker.html)
- [The Netflix Tech Blog](http://techblog.netflix.com/2012/02/fault-tolerance-in-high-volume.html)
- [Release It!](http://pragprog.com/book/mnee/release-it)

[![GoDoc](https://godoc.org/github.com/rubyist/circuitbreaker?status.svg)](https://godoc.org/github.com/rubyist/circuitbreaker)

## Installation

```
  go get github.com/rubyist/circuitbreaker
```

## Examples

Here is a quick example of what circuitbreaker provides

```go
// Creates a circuit breaker that will trip if the function fails 10 times
cb := circuit.NewThresholdBreaker(10)

events := cb.Subscribe()
go func() {
  for {
    e := <-events
    // Monitor breaker events like BreakerTripped, BreakerReset, BreakerFail, BreakerReady
  }
}()

cb.Call(func() error {
	// This is where you'll do some remote call
	// If it fails, return an error
}, 0)
```

Circuitbreaker can also wrap a time out around the remote call.

```go
// Creates a circuit breaker that will trip after 10 failures
// using a time out of 5 seconds
cb := circuit.NewThresholdBreaker(10)

cb.Call(func() error {
  // This is where you'll do some remote call
  // If it fails, return an error
}, time.Second * 5) // This will time out after 5 seconds, which counts as a failure

// Proceed as above

```

Circuitbreaker can also trip based on the number of consecutive failures.

```go
// Creates a circuit breaker that will trip if 10 consecutive failures occur
cb := circuit.NewConsecutiveBreaker(10)

// Proceed as above
```

Circuitbreaker can trip based on the error rate.

```go
// Creates a circuit breaker based on the error rate
cb := circuit.NewRateBreaker(0.95, 100) // trip when error rate hits 95%, with at least 100 samples

// Proceed as above
```

If it doesn't make sense to wrap logic in Call(), breakers can be handled manually.

```go
cb := circuit.NewThresholdBreaker(10)

for {
  if cb.Ready() {
    // Breaker is not tripped, proceed
    err := doSomething()
    if err != nil {
      cb.Fail() // This will trip the breaker once it's failed 10 times
      continue
    }
    cb.Success()
  } else {
    // Breaker is in a tripped state.
  }
}
```

Circuitbreaker also provides a wrapper around `http.Client` that will wrap a
time out around any request.

```go
// Passing in nil will create a regular http.Client.
// You can also build your own http.Client and pass it in
client := circuit.NewHTTPClient(time.Second * 5, 10, nil)

resp, err := client.Get("http://example.com/resource.json")
```

See the godoc for more examples.

## Bugs, Issues, Feedback

Right here on GitHub: [https://github.com/rubyist/circuitbreaker](https://github.com/rubyist/circuitbreaker)
