# `go-geom` Internals


## Introduction

`go-geom` attempts to implement efficient, standards-compatible OGC-style
geometries for Go.  This document describes some of the key ideas required to
understand its implementation.

`go-geom` is an evolution of the techniques developed for the [OpenLayers 3
geometry library](http://openlayers.org/en/master/apidoc/ol.geom.html),
designed to efficiently handle large geometries in a resource-constrained,
garbage-collected environment, but adapted to the Go programming language and
its type system.


## Type flexibility 

There are three priniciple 2D geometry types: `Point`s, `LineString`s, and
`Polygon`s.

OGC extends these three into collections of the principle types: `MultiPoint`,
`MultiLineString`, and `MultiPolygon`.  This gives 3 geometry types * 2
multi-or-not-multi = 6 combinations.

On top of this, there are multiple combinations of dimensions, e.g. 2D (XY), 3D
(XYZ), 2D varying over time/distance (XYM), and 3D varying over time/distance
(XYZM).

3 geometry types * 2 multi-or-not-multi * 4 different dimensionalities = 24
distinct types.

Go has neither generics, nor macros, nor a rich type system. `go-geom` attempts
to manage this combinatorial explosion while maintaining an idiomatic Go API,
implementation efficiency. and high runtime performance.


## Structural similarity

`go-geom` exploits structural similarity between different geometry types to
share code. Consider:

0.  A `Point` consists of a single coordinate. This single coordinate is a
    `geom.Coord`.

1.  A `LineString`, `LinearRing`, and `MultiPoint` consist of a collection of
    coordinates. They all have different semantics (a `LineString` is ordered,
a `LinearRing` is ordered and closed, a `MultiPoint` is neither ordered nor
closed) yet all share a similar underlying structure.

2.  A `Polygon` and a `MultiLineString` are a collection of collections of
    coordinates. Again, the semantics vary: a `Polygon` is a weakly ordered
collection of `LinearRing`s (the first `LinearRing` is the outer boundary,
subsequent `LinearRing`s are inner boundaries (holes)). A `MultiLineString` is
an unordered collection of `LineString`s.

3.  A `MultiPolygon` is an unordered collection of `Polygon`s.

`go-geom` makes these structural similarities explicit:

0. A `Point` is a `geom.Coord`, also known as `geom0`.

1. `LineString`s, `LinearRing`s, and and `MultiPoint`s are `[]geom.Coord`, also
   known as `geom1`.

2. `Polygon`s and `MultiLineString`s are `[][]geom.Coord`, also known as
   `geom2`.

3. `MultiPolygon`s are `[][][]geom.Coord`, also known as `geom3`.

Under the hood, `go-geom` uses Go's structural composition to share common
code. For example, `LineString`s, `LinearRing`s, and `MultiPoint`s all embed a
single anonymous `geom1`.

The hierarchy of embedding is:

	geom0
	+- geom1
	   +- geom2
	   +- geom3

Note that `geom2` and `geom3` independently embed `geom1`. Despite their
numerical ordering, `geom2` and `geom3` are separate branches of the geometry
tree.

We can exploit these structural similarities to share code. For example,
calculating the bounds of a geometry only involves finding the minimum and
maximum values in each dimension, which can be found by iterating over all
coordinates in the geometry. The semantic meaning of these coordinates -
whether they're points on a line, or points on a polygon inner or outer
boundary, or something else - does not matter. Therefore, as long as we can
treat any geometry as a collection of coordinates, we can use the same code to
calculate bounds across all geometry types.

Similarly, we can exploit higher-level similarities. For example, the "length"
of a `MultiLineString` is the sum of the lengths of its component
`LineString`s, and the "length" (perimeter) of a `Polygon` is the sum of the
lengths (perimeters) of its component `LinearRing`s.


## Efficient 

At the time of writing (2016), CPUs are fast, cache hits are quite fast, cache
misses are slow, memory is very slow, and garbage collection takes an eternity.

Typical geometry libraries use multiple levels of nested arrays, e.g. a
`[][][]float64` for a polygon. This requires multiple levels of indirection to
access a single coordinate value, and as different sub-arrays might be stored
in different parts of memory, is more likely to lead to cache miss.

In contrast, `go-geom` packs all the coordinates for a geometry, whatever its
structure, into a single `[]float64`. The underlying array is stored in a
single blob of memory. Most operations do a linear scan over the array, which
is particularly cache friendly. There are also fewer objects for the garbage
collector to manage.

Parts of the underlying array can be shared between multitple objects. For
example, retrieving the outer ring of a `Polygon` returns a `LinearRing` that
references the coordinates of the `Polygon`. No coordinate data are copied.
