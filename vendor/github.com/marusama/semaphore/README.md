semaphore
=========
[![Awesome](https://cdn.rawgit.com/sindresorhus/awesome/d7305f38d29fed78fa85652e3a63e154dd8e8829/media/badge.svg)](https://github.com/avelino/awesome-go#goroutines)
[![Build Status](https://travis-ci.org/marusama/semaphore.svg?branch=master)](https://travis-ci.org/marusama/semaphore)
[![Go Report Card](https://goreportcard.com/badge/github.com/marusama/semaphore)](https://goreportcard.com/report/github.com/marusama/semaphore)
[![Coverage Status](https://coveralls.io/repos/github/marusama/semaphore/badge.svg?branch=master)](https://coveralls.io/github/marusama/semaphore?branch=master)
[![GoDoc](https://godoc.org/github.com/marusama/semaphore?status.svg)](https://godoc.org/github.com/marusama/semaphore)
[![License](https://img.shields.io/github/license/mashape/apistatus.svg?maxAge=2592000)](LICENSE)

Fast resizable golang semaphore based on CAS

* allows weighted acquire/release;
* supports cancellation via context;
* allows change semaphore limit after creation;
* faster than channel based semaphores.

### Usage
Initiate
```go
import "github.com/marusama/semaphore"
...
sem := semaphore.New(5) // new semaphore with limit = 5
```
Acquire
```go
sem.Acquire(ctx, n)     // acquire n with context
sem.TryAcquire(n)       // try acquire n without blocking 
...
ctx := context.WithTimeout(context.Background(), time.Second)
sem.Acquire(ctx, n)     // acquire n with timeout
``` 
Release
```go
sem.Release(n)          // release n
```
Change semaphore limit
```go
sem.SetLimit(new_limit) // set new semaphore limit
```


### Some benchmarks
Run on MacBook Pro (2017) with 3,1GHz Core i5 cpu and 8GB DDR3 ram, macOS High Sierra, go version go1.11.4 darwin/amd64:
```text
// this semaphore:
BenchmarkSemaphore_Acquire_Release_under_limit_simple-4                   	20000000	        98.6 ns/op	      96 B/op	       1 allocs/op
BenchmarkSemaphore_Acquire_Release_under_limit-4                          	 1000000	      1593 ns/op	     960 B/op	      10 allocs/op
BenchmarkSemaphore_Acquire_Release_over_limit-4                           	  100000	     20760 ns/op	    9600 B/op	     100 allocs/op


// some other implementations:

// golang.org/x/sync/semaphore:
BenchmarkXSyncSemaphore_Acquire_Release_under_limit_simple-4              	50000000	        34.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkXSyncSemaphore_Acquire_Release_under_limit-4                     	 1000000	      1103 ns/op	       0 B/op	       0 allocs/op
BenchmarkXSyncSemaphore_Acquire_Release_over_limit-4                      	   30000	     65927 ns/op	   15985 B/op	     299 allocs/op

// github.com/abiosoft/semaphore:
BenchmarkAbiosoftSemaphore_Acquire_Release_under_limit_simple-4           	10000000	       208 ns/op	       0 B/op	       0 allocs/op
BenchmarkAbiosoftSemaphore_Acquire_Release_under_limit-4                  	  500000	      3147 ns/op	       0 B/op	       0 allocs/op
BenchmarkAbiosoftSemaphore_Acquire_Release_over_limit-4                   	   50000	     37148 ns/op	       0 B/op	       0 allocs/op

// github.com/dropbox/godropbox
BenchmarkDropboxBoundedSemaphore_Acquire_Release_under_limit_simple-4     	20000000	        75.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkDropboxBoundedSemaphore_Acquire_Release_under_limit-4            	 2000000	       629 ns/op	       0 B/op	       0 allocs/op
BenchmarkDropboxBoundedSemaphore_Acquire_Release_over_limit-4             	  200000	     27308 ns/op	       0 B/op	       0 allocs/op
BenchmarkDropboxUnboundedSemaphore_Acquire_Release_under_limit_simple-4   	50000000	        39.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkDropboxUnboundedSemaphore_Acquire_Release_under_limit-4          	 1000000	      1170 ns/op	       0 B/op	       0 allocs/op
BenchmarkDropboxUnboundedSemaphore_Acquire_Release_over_limit-4           	  100000	     21013 ns/op	       0 B/op	       0 allocs/op

// github.com/kamilsk/semaphore
BenchmarkKamilskSemaphore_Acquire_Release_under_limit_simple-4            	20000000	       110 ns/op	      16 B/op	       1 allocs/op
BenchmarkKamilskSemaphore_Acquire_Release_under_limit-4                   	 1000000	      1520 ns/op	     160 B/op	      10 allocs/op
BenchmarkKamilskSemaphore_Acquire_Release_over_limit-4                    	   50000	     42693 ns/op	    1600 B/op	     100 allocs/op

// github.com/pivotal-golang/semaphore
BenchmarkPivotalGolangSemaphore_Acquire_Release_under_limit_simple-4      	 3000000	       558 ns/op	     136 B/op	       2 allocs/op
BenchmarkPivotalGolangSemaphore_Acquire_Release_under_limit-4             	  200000	      9530 ns/op	    1280 B/op	      20 allocs/op
BenchmarkPivotalGolangSemaphore_Acquire_Release_over_limit-4              	   10000	    111264 ns/op	   12801 B/op	     200 allocs/op

```
You can rerun these benchmarks, just checkout `benchmarks` branch and run `go test -bench=. -benchmem ./bench/...`
