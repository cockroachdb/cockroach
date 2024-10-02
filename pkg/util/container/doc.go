// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package container provides three container types: heap, list, and ring.

The package is a direct copy of the Go standard library's container package,
but with interfaces replaced with generics. No other changes have been made,
even at the expense of ergonomics in a few places, and no other changes should
be made. The intention is to delete this library when the Go standard library
adds generics support to these packages.

The use of generics over (empty) interfaces for the contained value in these
containers provides three main benefits:

  - type safety: generics replace dynamic type assertions with static type
    checks. This ensures that code with type errors cannot compile, rather than
    panicking at runtime.

  - performance: generics allow values to be stored directly in the container,
    rather than boxed in an interface{}. This reduces allocations, eliminates
    runtime type assertions, and improves cache locality.

  - code documentation: generics require the container to be parameterized by
    the type of value it stores. This serves as a helpful form of documentation
    that describes the intended use of the container.

Container types are one of the quintessential application of generics, so much
of the benefit of generics that was described in the original design proposal[1]
is realized by this package.

[1]: https://go.googlesource.com/proposal/+/HEAD/design/43651-type-parameters.md
*/
package container
