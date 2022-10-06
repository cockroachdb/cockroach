# pkg/json [![GoDoc](https://godoc.org/github.com/pkg/json?status.svg)](https://godoc.org/github.com/pkg/json)

An alternative JSON decoder for Go.

# Note: This is a fork of pkg/json package

## Features

`pkg/json` aims to be a drop in replacement for `encoding/json`.
It features:

- `json.Scanner` which, when provided an external buffer, does not allocate.
- `io.Reader` friendly; you don't need to buffer your input in memory.
- `json.Decoder.Token()` replacement that is 2-3x faster than `encoding/json`.
- `json.Decoder.NextToken()` is _almost_ allocation free.

## Is it faster than fastjson/ultrajson/megajson/fujson?

Honestly, I don't know.
I have some benchmarks that show that `pkg/json` is faster than `encoding/json` for tokenisation, but this package isn't finished yet.


The `Decoder.Token` API is between 2-3x faster than `encoding/json.Decoder.Token`:
```
BenchmarkDecoderToken/pkgjson/canada.json.gz-16                       51          21387427 ns/op         105.25 MB/s     4402975 B/op     222279 allocs/op
BenchmarkDecoderToken/encodingjson/canada.json.gz-16                  18          63962471 ns/op          35.19 MB/s    17740379 B/op     889106 allocs/op
BenchmarkDecoderToken/pkgjson/citm_catalog.json.gz-16                235           5110849 ns/op         337.95 MB/s      966008 B/op      81995 allocs/op
BenchmarkDecoderToken/encodingjson/citm_catalog.json.gz-16            61          19635188 ns/op          87.96 MB/s     5665660 B/op     324799 allocs/op
BenchmarkDecoderToken/pkgjson/twitter.json.gz-16                     495           2424694 ns/op         260.45 MB/s      768370 B/op      38992 allocs/op
BenchmarkDecoderToken/encodingjson/twitter.json.gz-16                 98          12028296 ns/op          52.50 MB/s     3660269 B/op     187815 allocs/op
BenchmarkDecoderToken/pkgjson/code.json.gz-16                         75          16111648 ns/op         120.44 MB/s     4304252 B/op     320235 allocs/op
BenchmarkDecoderToken/encodingjson/code.json.gz-16                    15          75464079 ns/op          25.71 MB/s    23355964 B/op    1319125 allocs/op
BenchmarkDecoderToken/pkgjson/example.json.gz-16                   23210             51699 ns/op         251.92 MB/s       16032 B/op        914 allocs/op
BenchmarkDecoderToken/encodingjson/example.json.gz-16               4767            257169 ns/op          50.64 MB/s       82416 B/op       4325 allocs/op
BenchmarkDecoderToken/pkgjson/sample.json.gz-16                     1411            849803 ns/op         809.00 MB/s      213776 B/op       5081 allocs/op
BenchmarkDecoderToken/encodingjson/sample.json.gz-16                 345           3550453 ns/op         193.63 MB/s      759686 B/op      26643 allocs/op
```

The alternative `Decoder.NextToken` API is between 8-10x faster than `encoding/json.Decoder.Token` while producing virtually no allocations:
```
BenchmarkDecoderNextToken/pkgjson/canada.json.gz-16                  222           5157299 ns/op         436.48 MB/s         152 B/op          3 allocs/op
BenchmarkDecoderNextToken/encodingjson/canada.json.gz-16              18          67566098 ns/op          33.32 MB/s    17740510 B/op     889106 allocs/op
BenchmarkDecoderNextToken/pkgjson/citm_catalog.json.gz-16            510           2260514 ns/op         764.08 MB/s         152 B/op          3 allocs/op
BenchmarkDecoderNextToken/encodingjson/citm_catalog.json.gz-16        61          20026083 ns/op          86.25 MB/s     5665620 B/op     324799 allocs/op
BenchmarkDecoderNextToken/pkgjson/twitter.json.gz-16                1032           1093466 ns/op         577.53 MB/s         168 B/op          4 allocs/op
BenchmarkDecoderNextToken/encodingjson/twitter.json.gz-16             93          12548028 ns/op          50.33 MB/s     3660291 B/op     187815 allocs/op
BenchmarkDecoderNextToken/pkgjson/code.json.gz-16                    211           5322161 ns/op         364.60 MB/s         264 B/op          6 allocs/op
BenchmarkDecoderNextToken/encodingjson/code.json.gz-16                14          78195551 ns/op          24.82 MB/s    23355961 B/op    1319125 allocs/op
BenchmarkDecoderNextToken/pkgjson/example.json.gz-16               49245             22873 ns/op         569.41 MB/s         168 B/op          4 allocs/op
BenchmarkDecoderNextToken/encodingjson/example.json.gz-16           4500            269313 ns/op          48.36 MB/s       82416 B/op       4325 allocs/op
BenchmarkDecoderNextToken/pkgjson/sample.json.gz-16                 2032            553729 ns/op        1241.57 MB/s        1160 B/op          9 allocs/op
BenchmarkDecoderNextToken/encodingjson/sample.json.gz-16             332           3574484 ns/op         192.33 MB/s      759685 B/op      26643 allocs/op
```

Decoding into an `interface{}` this package is 40-50% faster than `encoding/json`:
```
BenchmarkDecoderDecodeInterfaceAny/pkgjson/canada.json.gz-16                  43          27181309 ns/op          82.82 MB/s     8747225 B/op     281408 allocs/op
BenchmarkDecoderDecodeInterfaceAny/encodingjson/canada.json.gz-16             30          36930290 ns/op          60.95 MB/s    20647699 B/op     392553 allocs/op
BenchmarkDecoderDecodeInterfaceAny/pkgjson/citm_catalog.json.gz-16           156           7635508 ns/op         226.21 MB/s     5197871 B/op      89673 allocs/op
BenchmarkDecoderDecodeInterfaceAny/encodingjson/citm_catalog.json.gz-16       73          15766855 ns/op         109.55 MB/s     9410808 B/op      95496 allocs/op
BenchmarkDecoderDecodeInterfaceAny/pkgjson/twitter.json.gz-16                327           3664618 ns/op         172.33 MB/s     2130822 B/op      30182 allocs/op
BenchmarkDecoderDecodeInterfaceAny/encodingjson/twitter.json.gz-16           175           6800730 ns/op          92.86 MB/s     4359944 B/op      31775 allocs/op
BenchmarkDecoderDecodeInterfaceAny/pkgjson/code.json.gz-16                    64          17302914 ns/op         112.15 MB/s     7331655 B/op     232059 allocs/op
BenchmarkDecoderDecodeInterfaceAny/encodingjson/code.json.gz-16               49          23759564 ns/op          81.67 MB/s    12332748 B/op     271292 allocs/op
BenchmarkDecoderDecodeInterfaceAny/pkgjson/example.json.gz-16              15920             76561 ns/op         170.11 MB/s       50988 B/op        739 allocs/op
BenchmarkDecoderDecodeInterfaceAny/encodingjson/example.json.gz-16          8670            139230 ns/op          93.54 MB/s       82840 B/op        782 allocs/op
BenchmarkDecoderDecodeInterfaceAny/pkgjson/sample.json.gz-16                1084           1119477 ns/op         614.12 MB/s      399983 B/op       5542 allocs/op
BenchmarkDecoderDecodeInterfaceAny/encodingjson/sample.json.gz-16            217           5418480 ns/op         126.88 MB/s     2697781 B/op       8075 allocs/op
```

## Should I use this?

Right now? No.
In the future, maybe.

## I've found a bug!

Great! Please follow this two step process:

1. Raise an issue
2. Send a PR (unless I fix it first)

## Standing on the shoulders of giants

This project is heavily influenced by Steven Schveighoffer's [`iopipe`](https://www.youtube.com/watch?v=un-bZdyumog) and [Phil Pearl](https://philpearl.github.io/post/reader/).
