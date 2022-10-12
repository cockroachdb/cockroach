# backoff ![](https://github.com/lestrrat-go/backoff/workflows/CI/badge.svg) [![Go Reference](https://pkg.go.dev/badge/github.com/lestrrat-go/backoff/v2.svg)](https://pkg.go.dev/github.com/lestrrat-go/backoff/v2)

Idiomatic backoff for Go

This library is an implementation of backoff algorithm for retrying operations
in an idiomatic Go way. It respects `context.Context` natively, and the critical
notifications are done through *channel operations*, allowing you to write code 
that is both more explicit and flexibile.

For a longer discussion, [please read this article](https://medium.com/@lestrrat/yak-shaving-with-backoff-libraries-in-go-80240f0aa30c)

# IMPORT

```go
import "github.com/lestrrat-go/backoff/v2"
```

# SYNOPSIS

```go
func ExampleExponential() {
  p := backoff.Exponential(
    backoff.WithMinInterval(time.Second),
    backoff.WithMaxInterval(time.Minute),
    backoff.WithJitterFactor(0.05),
  )

  flakyFunc := func(a int) (int, error) {
    // silly function that only succeeds if the current call count is
    // divisible by either 3 or 5 but not both
    switch {
    case a%15 == 0:
      return 0, errors.New(`invalid`)
    case a%3 == 0 || a%5 == 0:
      return a, nil
    }
    return 0, errors.New(`invalid`)
  }

  ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
  defer cancel()

  retryFunc := func(v int) (int, error) {
    b := p.Start(ctx)
    for backoff.Continue(b) {
      x, err := flakyFunc(v)
      if err == nil {
        return x, nil
      }
    }
    return 0, errors.New(`failed to get value`)
  }

  retryFunc(15)
}
```

# POLICIES

Policy objects describe a backoff policy, and are factories to create backoff Controller objects.
Controller objects does the actual coordination.
Create a new controller for each invocation of a backoff enabled operation.
This way the controller object is protected from concurrent access (if you have one) and can easily be discarded

## Null

A null policy means there's no backoff. 

For example, if you were to support both using and not using a backoff in your code you can say

```go
  var p backoff.Policy
  if useBackoff {
    p = backoff.Exponential(...)
  } else {
    p = backoff.Null()
  }
  c := p.Start(ctx)
  for backoff.Continue(c) {
    if err := doSomething(); err != nil {
      continue
    }
    return
  }
```

Instead of

```go
  if useBackoff {
    p := backoff.Exponential(...)
    c := p.Start(ctx)
    for backoff.Continue(c) {
      if err := doSomething(); err != nil {
        continue
      }
      return
    }
  } else {
    if err := doSomething(); err != nil {
      continue
    }
  }
```

## Constant

A constant policy implements are backoff where the intervals are always the same

## Exponential

This is the most "common" of the backoffs. Intervals between calls are spaced out such that as you keep retrying, the intervals keep increasing.

# FAQ

## I'm getting "package github.com/lestrrat-go/backoff/v2: no Go files in /go/src/github.com/lestrrat-go/backoff/v2"

You are using Go in GOPATH mode, which was the way before [Go Modules](https://blog.golang.org/using-go-modules) were introduced in Go 1.11 (Aug 2018).
GOPATH has slowly been phased out, and in Go 1.14 onwards, Go Modules pretty much Just Work.
Go 1.16 introduced more visible changes that forces users to be aware of the existance of go.mod files.

The short answer when you you get the above error is: **Migrate to using Go Modules**.
This is simple: All you need to do is to include a go.mod (and go.sum) file to your library or app.

For example, if you have previously been doing this:

```
git clone git@github.com:myusername/myawesomeproject.git
cd myawesomeproject
go get ./...
```

First include go.mod and go.sum in your repository:

```
git clone git@github.com:myusername/myawesomeproject.git
cd myawesomeproject
go mod init
go mod tidy
git add go.mod go.sum
git commit -m "Add go.mod and go.sum" -a
git push 
```

Then from subsequent calls:

```
git clone git@github.com:myusername/myawesomeproject.git
cd myawesomeproject
go build # or go test, or go run, or whatever.
```

This will tell go to respect tags, and will automatically pick up the latest version of github.com/lestrrat-go/backoff

If you really can't do this, then the quick and dirty workaround is to just copy the files over to /v2 directory of this library

```
BACKOFF=github.com/lestrrat-go/backoff
go get github.com/lestrrat-go/backoff
if [[ if ! -d "$GOPATH/src/$BACKOFF/v2" ]]; then
  mkdir "$GOPATH/src/$BACKOFF/v2" # just to make sure it exists
fi
cp "$GOPATH/src/$BACKOFF/*.go" "$GOPATH/src/$BACKOFF/v2"

git clone git@github.com:myusername/myawesomeproject.git
cd myawesomeproject
go get ./...
```

## Why won't you just add the files in /v2?

Because it's hard to maintain multiple sources of truth. Sorry, I don't get paid to do this.
I will not hold anything against you if you decided to fork this to your repository, and move files to your own /v2 directory.
Then, if you have a go.mod in your app, you can just do

```
go mod edit -replace github.com/lestrrat-go/backoff/v2=github.com/myusername/myawesomemfork/v2
```

Oh, wait, then you already have go.mod, so this is a non-issue. 

...Yeah, just migrate to using go modules, please?

