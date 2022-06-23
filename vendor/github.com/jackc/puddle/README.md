[![](https://godoc.org/github.com/jackc/puddle?status.svg)](https://godoc.org/github.com/jackc/puddle)
[![Build Status](https://travis-ci.org/jackc/puddle.svg)](https://travis-ci.org/jackc/puddle)

# Puddle

Puddle is a tiny generic resource pool library for Go that uses the standard
context library to signal cancellation of acquires. It is designed to contain
the minimum functionality required for a resource pool. It can be used directly
or it can be used as the base for a domain specific resource pool. For example,
a database connection pool may use puddle internally and implement health checks
and keep-alive behavior without needing to implement any concurrent code of its
own.

## Features

* Acquire cancellation via context standard library
* Statistics API for monitoring pool pressure
* No dependencies outside of standard library
* High performance
* 100% test coverage

## Example Usage

```go
constructor := func(context.Context) (interface{}, error) {
  return net.Dial("tcp", "127.0.0.1:8080")
}
destructor := func(value interface{}) {
  value.(net.Conn).Close()
}
maxPoolSize := 10

pool := puddle.NewPool(constructor, destructor, maxPoolSize)

// Acquire resource from the pool.
res, err := pool.Acquire(context.Background())
if err != nil {
  // ...
}

// Use resource.
_, err = res.Value().(net.Conn).Write([]byte{1})
if err != nil {
  // ...
}

// Release when done.
res.Release()

```

## License

MIT
