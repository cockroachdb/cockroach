- Feature Name: Geospatial
- Status: in-progress
- Start Date: 2020-04-01
- Authors: danhhz otan rytaft sumeerbhola
- RFC PR: [#47762](https://github.com/cockroachdb/cockroach/pull/47762)
- Cockroach Issue: [#19313](https://github.com/cockroachdb/cockroach/issues/19313)

Table of contents:

- [Summary](#Summary)
- [Guide-level explanation](#Guide-level-explanation)
- [Reference-level explanation](#Reference-level-explanation)
  * [SQL Types & Functions](#SQL-Types--Functions)
  * [Indexing](#Indexing)
  * [End-to-End Example](#End-to-End-Example)
  * [Package Structure](#Package-Structure)
  * [Telemetry](#Telemetry)
  * [Testing](#Testing)
  * [Performance Benchmarking](#Performance-Benchmarking)
- [Unresolved questions](#Unresolved-questions)

# Summary

This RFC proposes the addition of geospatial features to CockroachDB, similar to
what is supported by PostGIS. These features are often requested by users. At the time
of writing, support for geospatial features is the most requested feature in
CockroachDB.

The approach leverages open-source geometry libraries used by PostGIS whenever
possible. For indexing, the approach diverges from PostGIS by dividing the space
into cells of decreasing size, which fits into the totally ordered key-value
model of CockroachDB. Early experiments suggest that this is competitive in
performance with R-trees whilst allowing for horizontal scaling.

# Guide-level explanation

Our geospatial implementation will be compatible with PostGIS. There
are a number of SQL geospatial implementations that we could have
modeled ourselves after as well as the OGC SQL standard or we could
have developed our own new interface. We've selected PostGIS
compatibility largely for two reasons:

1. When polled directly, a large fraction of our users asked for
   PostGIS compatibility.
2. We've found time and time again that being a drop in replacement
   for Postgres is an important aid to adoption. Given that the
   open-source geospatial community has built most of its tooling around
   PostGIS, this has the potential to be an even larger impact than
   usual.

Additionally, both PostGIS and SQL Server largely follow the OGC SQL
standard, so this is not really as opinionated a decision as it would
otherwise be. However, notably, where PostGIS behavior diverges from
the OGC SQL standard, we've opted to prioritize the PostGIS behavior.

As a result, our initial user-facing geospatial footprint will be a
subset of PostGIS's footprint. We'll certainly expand this footprint
over time, but given that some PostGIS features are rarely used
(raster) and others are deprecated, it's likely that we'll never cover
all of it. See below for the exact initial footprint we'll be aiming
for.

The specs we refer to in this document are:

* OGC: [v1.1](http://portal.opengeospatial.org/files/?artifact_id=13228)
** NOTE: For 20.2, we will aim to support v1.1 as PostGIS does
* SQL/MM: [ISO 13249-3](http://jtc1sc32.org/doc/N1101-1150/32N1107-WD13249-3--spatial.pdf)

Our approach will leverage the open-source geometry libraries used by
PostGIS whenever possible. These libraries are widely used and tested,
and additionally allow us to match PostGIS behavior for geometric
corner cases. For indexing, we will diverge from PostGIS, which uses
R-trees. The totally ordered key-value model used in CockroachDB
indexes allows for horizontal scaling and continuous repartitioning,
and we need to preserve these fundamental characteristics of the
system. We will use a divide-the-space approach that divides space
into cells of decreasing size, and turns the cell space into a linear
ordering using a space-filling curve. Shapes are indexed by a set of
cells using an inverted index.

# Reference-level explanation

Geospatial is exposed through two new SQL types: GEOMETRY (planar) and
GEOGRAPHY (sphere/spheroid, which only works on latitude/longitude
SRID systems). SQL Datums can be either of these types, as can SQL table
columns.

An individual datum of type GEOMETRY or GEOGRAPHY will be one of
several "shapes". The initial set of these we'll support (and is all
that is supported by GEOGRAPHY) is:

* POINT
* LINESTRING
* POLYGON
* MULTIPOINT
* MULTILINESTRING
* MULTIPOLYGON
* GEOMETRYCOLLECTION
* GEOMETRY ("shape", which is different than the "type", which allows
any of the above types to be referenced - more later)

In addition, geospatial types and datums can have Spatial Reference
System Identifiers (SRIDs) which specify which projection was applied
to their coordinate values. At a base level, we will look to support
SRID 0 (undefined) and 4326 (vanilla lat/lng geography on a WGS84
spheroid). Supporting SRIDs outside of these two is a stretch task for
the 20.2 release which we will detail in this RFC.

A number of new SQL builtin functions are added which operate on
GEOMETRY and GEOGRAPHY. Some of these functions can be accelerated by
maintaining a spatial index over a table, which is exposed as a SQL
index. The optimizer will be aware of these indexes and use them in
planning.


## SQL Types & Functions

### SQL Types

We will introduce two new data types to support geospatial data:

* Geometry - for planar functionality compliant with the OGC spec. For
  20.2, we will look at only supporting 2D geometries, but still
  support ingesting Z- and M- coordinates. For 20.2, Geometry will be
  a wrapper around the
  [twpayne/go-geom](https://github.com/twpayne/go-geom) library
  which allows us to easily transform the datum for use in
  the GEOS/SFCGAL libraries that have the builtins we require (see the
  builtins section), whilst also allowing some simple operations to be
  done without converting to GEOS/SFCGAL. We will gradually
  switch to our own abstraction as we look to support curves/3D
  geometries in future versions, which is lacking in twpayne/go-geom.

* Geography - for a subset of functionality of geometry that does
  calculations on a spheroid using geodesic lines, operating only on
  lat/lng-based projections (see the "Spatial Reference Identifiers
  (SRIDs) and Projections" section). This is not part of the OGC
  spec. For 20.2, Geography will mostly wrap around the
  [S2 Geometry](https://s2geometry.io/) Region interface and
  GeographicLib. The S2 Geometry will give us sphere math, as well as
  operations which do not require physical real world
  values. GeographicLib will perform necessary spheroid math
  operations which require real world values (i.e. in metres for
  ST_Area, ST_DWithin, ST_Distance, ST_Perimeter, or in degrees for
  ST_Azimuth).

Note we will not support box2d and box3d PostGIS types and operators,
as they are designed to work with the R-Tree index which we are not
supporting.


#### Supported Geometry Types (Shapes)

For each data type, we will be able to support the "geometry types" as
defined in Section 6.2.6 of the OGC spec. We will internally call
these geometry types "shapes". The full hierachy is from the OGC
spec is as follows.

![OGC Shape Hierachy](20200421_geospatial/shape_hierachy.png?raw=true "OGC Spec Hierachy")

We are planning to support the following:

* Initially, we will look to support the following shapes that are
  applicable to both Geometry and Geography in line with the OGC and
  SQL/MM specifications:
  * POINT
  * LINESTRING,
  * POLYGON
  * MULTIPOINT
  * MULTILINESTRING
  * MULTIPOLYGON
  * GEOMETRY (wildcard shape; can be any shape except another GEOMETRY) 
  * GEOMETRYCOLLECTION (can be a collection of all the MULTI- shapes)
  
* In the future, we will look to support the following shapes for the
  Geometry data type only, in line with PostGIS support:
  * 2.5D (all 2D with Z- and M- coordinates and 3D datatypes)
  * 2D:
    * CURVE
    * MULTICURVE
    * TRIANGLE
  * 3D: 
    * TIN (triangulated irregular network)
    * SURFACE
    * MULTISURFACE
    * POLYHEDRALSURFACE


#### OIDs

The OIDs for these data types do not exist in `lib/pq/oid` as PostGIS
is an extension which only adds this metadata when the extension is
installed in the CLI. We hence have to define our own OIDs for
geospatial data types.

We will hardcode them in a number range which fits in the range that
can fit within an int4. We should not use an OID range higher than
int4 as this would be incompatible with Postgres.


#### Casts

PostGIS allows Geography types to be implicitly cast into Geometry
types in addition to explicit casts. However, Geometry types can only
be explicitly cast to Geography types.

We will be able to support the explicit casts easily. However, as
implicit casts are not supported in Cockroach (yet), we may have to
"double define" a few definitions of functions that cast Geography to
Geometry to emulate PostGIS. We will aim to support this as
"pseudo-implicit casts" for commonly used functions that are only
defined for Geometry (e.g. ST_SRID). The user also has the option to
explicitly cast Geography to Geometry before the builtins if we miss
any.

Casts between other supported types (`text` and `bytea`) will also
be initially supported via explicit casts, with implicit casts or
pseudo-implicit casts mentioned above implemented at a later date.


#### Column Definitions

In line with PostGIS, we will support the following column definition
mechanisms for GEOMETRY:

* `ADD COLUMN <colname> GEOMETRY` - this allows any kind of geometry
  to be added. As no SRID is specified in the column definition, the
  geometry by default will not have a SRID (which is defined as SRID =
  0).

* `ADD COLUMN <colname> GEOMETRY(shape)` - this allows only the shape
  defined to be stored in this column. However, the GEOMETRY shape
  allows any shape to be stored in the column, and the
  GEOMETRYCOLLECTION shape allows any MULTI- prefixed shape to be
  stored in the column. As above, any SRID is allowed in this column.

* `ADD COLUMN <colname> GEOMETRY(shape, srid)` - applies both the
  shape restriction above, as well as only allowing a specific SRID to
  be stored in the column. If an SRID is not defined with the Datum
  (i.e. has an SRID of 0), it's SRID will implicitly change to the
  SRID in the given column. Note having a column of GEOMETRY(shape, 0)
  where the SRID is 0 allows any SRID to be stored.

We will mirror the same definitions for GEOGRAPHY as we do for
GEOMETRY with one key difference - the default SRID will be 4326
(representing vanilla lat/lng on a WGS84 sphere), instead of leaving
it with an SRID of 0.

For 20.2, operations such as `ALTER TABLE ... SET DATA TYPE ...` may not be
permitted, but will be extended in later versions.


#### Index Creation

To maintain compatibility with PostGIS and existing tools, the syntax
to create indexes will involve the same syntax as using GiST, which will
initialize the indexes with the S2-backed inverted index underneath.

This PostGIS-backed index will look like the following:

```
CREATE INDEX [indexname] ON [tablename] USING GIST ( [geospatial_column] );
```

If we decide to support user-adjustable S2 parameters, we could
support a syntax like the following:

```
CREATE INVERTED INDEX [indexname] ON [tablename] ( [geospatial_column] ) WITH (s2_max_level = 30, ….)
-- or
CREATE INDEX [indexname] ON [tablename] USING GIST ( [geospatial_column] ) WITH (s2_max_level = 30, ….)
```

For 2D planar geometry there will be an axis-aligned rectangular bound
that all shapes that want index acceleration should fit in. The bound
will be specified at index creation time. By default this will use
the bounds defined by the SRID.

Bounds can be overwritten using WITH:

``` 
CREATE INVERTED INDEX [indexname] ON [tablename] ( [geospatial_column] ) WITH (geometry_min_x = X, ...)
```

#### Disk Serialization

For Geometry and Geography types, we will serialize the data as
a protobuf that contains the EWKB. This allows us flexibility in
changing libraries later, as well as encoding the necessary type
and shape metadata. The protobuf will appear as follows:

```
message SpatialObject {
  // EWKB is always stored in littleEndian order.
  bytes ewkb = 1;
  // SRID is denormalized from the EWKB.
  int32 srid = 2;
  // Shape is the Shape that is stored in the database.
  Shape shape = 3;
  // BBox bounding box for optimizing certain operations
  // depending on performance benchmarks.
  BoundingBox bbox = 4;
}

enum Shape {
  GEOMETRY = 1;
  POINT = 2;
  // ... etc.
};

message BoundingBox {
  float64 min_x = 1;
  float64 min_y = 2;
  float64 max_x = 3;
  float64 max_y = 4;
}
```

It is worth noting we could serialize Geography types with the S2
library. The S2 library would be fastest to transform serialization
and deserialization into S2 types (compared to EWKB). However, it is
missing important metadata such as 2.5D/3D (Z-) and M- coordinates, as
well as original shape and SRID metadata. There is a "Compressed"
serialization option for S2 objects which further improves disk usage,
however it is not yet ported to Go.

We will not support indexing geospatial types in default primary keys
and unique secondary indexes (as we do not for JSONB and ARRAYs today). This is
because we will not be able to match the PostGIS definition as it's based
on a hash of its internal data structure, which means we will
not be able to be a "drop-in" replacement here. If we could decide
on our own ordering, it will be based on the raw EWKB byte ordering.
This may change subject to further user consultation before the upcoming
v20.2 release.

We will however support using inverted indexes for geospatial indexing
-- see the indexing section for details.

#### Display

In line with PostGIS, we will display values from SELECT as EWKB-hex
strings.


### Builtins

PostGIS supported functionality is defined in [Section 14.11 of their
documentation](https://postgis.net/docs/PostGIS_Special_Functions_Index.html#PostGIS_TypeFunctionMatrix),
which is a superset of those available in section 7 of the OGC
spec. For 20.2, we will aim to support all the 2D geometry (except for
CURVE) and geography functionality. The priority will be for functions
available for use for indexing and functions already implemented by
GEOS/S2/GeographicLib.

Most operations involving multiple Geometries and/or Geographies can
only succeed if the SRIDs between the two data types are the same.


#### Geometry (2D)

PostGIS mainly defers difficult 2D geometry functionality to the
[GEOS](https://trac.osgeo.org/geos/) library - however, some operations
are done in PostGIS itself if it is not supported in GEOS.

We will aim to use CGO to interface with the GEOS library in
20.2. This is the most straightforward and quick way to set up as the
GEOS library specifically targets implementing the OGC spec.  This
provides us with coverage for many of the mathematical predicates
required for 2D geometry and lets us build a lot of functionality
quickly. We will eat the CGO overhead to be able to ship quickly.

We will use 
[dlopen](http://man7.org/linux/man-pages/man3/dlopen.3.html) and
[dlsym](https://pubs.opengroup.org/onlinepubs/009695399/functions/dlsym.html) to
interface with the GEOS library. This prevents us from statically
linking the GEOS library - instead, a CRDB user must install GEOS into
their environment. As a corollary of this limitation:

* We will initially not be supporting certain 2D Geometry features on
  CRDB on Windows for the 20.2 (we can decide to use the
  [LoadLibrary](https://docs.microsoft.com/en-gb/windows/win32/api/libloaderapi/nf-libloaderapi-loadlibrarya?redirectedfrom=MSDN)
  function in the future).

* Users will have to specify the path of their GEOS install when
  running a cockroach binary. This should be set as an array of paths
  which points to the location where the C bindings may be installed,
  with a sensible default that covers most operating system default
  locations. This is a potential source of confusion, which should be
  well documented.
  * We should include the GEOS library in the docker containers we ship.
  * We will include GEOS in the tarball and include instructions
    from the installation page to copy configure GEOS if Geospatial
    operations are required. This is similar to PostGIS where users
    would require extra steps by installing the PostGIS extension
    dependencies to perform Geospatial operations.

Example changed installation instructions:

```sh
$ wget -qO- https://binaries.cockroachdb.com/cockroach-v20.2.0.linux-amd64.tgz | tar  xvz
$ cp -i cockroach-v20.2.0.linux-amd64/cockroach /usr/local/bin/
# Optional step.
$ cp -i cockroach-v20.2.0.linux-amd64/lib/*.so /usr/local/lib/cockroach/
```

However, GEOS will still be loaded provided it is in the same directory under `lib`
in which CockroachDB was executed from, e.g.:
```
$ wget -qO- https://binaries.cockroachdb.com/cockroach-v20.2.0.linux-amd64.tgz | tar  xvz
$ cd cockroach-v20.2.0.linux-amd64
$ ls
lib
cockroach
$ ls lib
libgeos.so
libgeos_c.so
$ ./cockroach # this should have GEOS loaded.
```

If a user attempts to use a geospatial function without having the library installed,
they should see an error similar to the following:

```sql
$ SELECT ST_Area('POINT(1.0 1.0)'::geometry)
ERROR: geospatial functions are only supported if the GEOS module is installed
HINT: See http://cockroachdb.com/link/to/installation/instructions
```

If a user loads geospatial data then later does not have the module installed,
the above error will still appear.


#### Geometry (Curves)

Curves are implemented natively in PostGIS, with an [approximation of
~32 points per
curve](https://github.com/postgis/postgis/blob/master/liblwgeom/lwgeom_geos.c#L412) 
done when input into the GEOS library. We can look to use some of the
same math after v20.2.


#### Geometry (2.5D)

These are implemented natively in PostGIS with some GEOS support. We
can look at doing this after v20.2.


#### Geometry (3D)

PostGIS uses [SFCGAL](http://www.sfcgal.org/) to compute 3D
geometry. We will employ similar CGO techniques described in Geometry
2D, with the same downsides.


#### Geography

In PostGIS, spherical calculations are done within PostGIS, whilst
values requiring spheroid values (e.g. Distance, Area) are done using
[GeographicLib](https://geographiclib.sourceforge.io/).

Most spherical calculations are supported by the S2 Geometry library
in C++, most of which are ported over to Golang. Combining the S2
Geometry library with using CGO/GeographicLib (which does not require
dlopen/dlsym as we can link it directly in our binaries), we will be
able to support all relevant Geography functions.


##### Default Args for Geography functions

Most geography functions that can optionally operate on a spheroid
(e.g. [ST_Area](https://postgis.net/docs/ST_Area.html)) have a
default argument of "use_spheroid = True". We do not support default
arguments in Cockroach. As such, we will have to define two functions
for each of these - one taking no bool and one taking a bool
value. This will add an extra "row" for this builtin in our docs
compared to PostGIS.

Users and tools specifying explicitly default args (e.g. `SELECT
ST_Area(geom, use_spheroid := false)`) will be unable to do so at v20.2
unless we implement the default args feature before then.


#### Comparison Ops

Direct comparison using the `>`/`>=`/`=`/`<`/`<=`/`!=` comparators in
PostGIS involve hashing the internal structure of the PostGIS
object. Comparators (besides =, which should probably use ST_Equal
anyway) are not expected to be commonly used with geospatial data
types.

As such, we will define our own comparator operations to use raw byte
EWKB comparisons, which will be incompatible with PostGIS for base
comparator operators only.


#### bbox Ops

PostGIS has a set of [bounding box
operators](https://postgis.net/docs/reference.html#idm9874) that are
not part of the OGC or SQL/MM specs. As mentioned above, our different
indexing approach prevents these from being index
accelerated. We may decide to support these at a later date.


#### Distance Operators

PostGIS has a set of [distance
operators](https://postgis.net/docs/reference.html#idm10964) that are
useful for accelerating k-nearest-neighbor search as outlined in
[https://postgis.net/workshops/postgis-intro/knn.html](https://postgis.net/workshops/postgis-intro/knn.html). We
plan to add support for k-nearest-neighbor in later versions, and may
support a subset of these distance operators.


#### Index Usage

Certain function builtins in PostGIS can utilize an index which we
will match with some optimizer work around the indexing (see Indexing
section).

For 2D geometry and geography, these are:

* ST_Covers
* ST_CoveredBy
* ST_Contains (geometry only)
* ST_ContainsProperly (geometry only)
* ST_Crosses (geometry only)
* ST_DFullyWithin (geometry only)
* ST_DWithin
* ST_Equals (geometry only)
* ST_Intersects
* ST_Overlaps (geometry only)
* ST_Touches (geometry only)
* ST_Within (geometry only)

These functions have an equivalent with an `_` prefix
(e.g. `_ST_Within`) that avoids using the indexes. We will similarly
have such functions, in which the optimizer will know not to use the
indexes when doing these operations.

3D geometry support includes 2D and 1D shapes embedded in 3 coordinate
dimensions, and some support for 3D shapes (solids). This uses the
SFCGAL library. We will not support 3D coordinate dimensions since we
do not yet have a comprehensive solution for indexing 3D space. For
reference, the 3D functions that need such indexing support are:

* ST_3DDFullyWithin
* ST_3DDWithin
* ST_3DIntersects


#### Builtin Result Caching

In PostGIS, certain builtin operations are cached, using the hashed data
from the shape as keys. This is especially useful for ST_Distance/ST_DWithin
for repeated use of these operations. We may similarly look into
caching the results of certain operations, but this is out of scope for
v20.2. If we decide to go along this route, a future RFC will be
published.

#### Documentation

PostGIS has documentation for each of the builtins they support, which
gives details such as which libraries are used, common gotchas and
links to other useful information (click on any builtin as defined in
[Section
14.11](https://postgis.net/docs/PostGIS_Special_Functions_Index.html#PostGIS_TypeFunctionMatrix)). We
should be similarly detailed and rigorous with our
explanations. However, we will need to build the necessary framework
to support referencing (valid) docs within our builtins for this to
work.


### External Formats

We will support the following external formats (and the associated
builtins for parsing and encoding them):

* Well Known Text (WKT) and Well Known Bytes (WKB), as defined in the
  SQL/MM Section 5.1.45 and 5.1.46. These are all supported by
  twpayne/geom library, except for WKT decoding. For the WKT decoding,
  we can use the GEOS library (which blocks Geography adoption for
  non-GEOS installs) in the interim, but as GEOS library does not
  support curves/3D types, we may wish to write our own parser in the
  future.

* Extended Well Known Text (EWKT) and Extended Well Known Bytes
  (EWKB), as defined by PostGIS for cross-compatibility. This contains
  an extra reserved bit for specifying whether an SRID is included, as
  well as extra bytes used for encoding the SRID itself. These are all
  supported by twpayne/geom library.

* GeoJSON, as defined by
  [RFC7946](https://tools.ietf.org/html/rfc7946). These are all
  supported by twpayne/go-geom libraries.

Furthermore, we will aim to be compatible with importing and exporting
other data external file types using the [`ogr2ogr`](https://gdal.org/programs/ogr2ogr.html)
tool.

Initially, these imports can be directly ingested as PGDUMPs using the
`IMPORT PGDUMP` syntax. An example workflow:

```
$ ogr2ogr -f PGDUMP cockroach-data/import.sql import.gpkg -skipfailures
$ echo "IMPORT PGDUMP 'nodelocal://0/import.sql" | cockroach sql 
```

For exports, a user can transform the dataset
into GeoJSON before using `ogr2ogr` to massage them into a different format.
An example workflow:

```
$ echo "SELECT json_build_object(
        'type', 'FeatureCollection',
        'name', 'nyc_subway_stations',
        'features', json_agg(st_asgeojson(nyc_subway_stations.*)::jsonb)
) FROM nyc_subway_stations;" | cockroach sql --format=raw | head -n 4 | tail -n 1 > ~/subway.json
$ ogr2ogr -f GPKG ~/subway.gpkg ~/subway.json 
```

Re-using `ogr2ogr` allows us to support file types such as:

* ESRI Shapefiles
* PGDUMPs
* Geodatabase
* Geopackage
* [and vector drivers mentioned here](https://gdal.org/drivers/vector/index.html)

### Spatial Reference Identifiers (SRIDs) and Projections

A SRID is a number that identifies a spatial reference system. A
spatial reference system provides meaning to the point coordinates
used in the representation of shapes. When the geometry type is
used to represent the Earth, the SRID represents a projection on
a plane, for example the
[web mercator](https://en.wikipedia.org/wiki/Web_Mercator_projection)
projection is represented using SRID 3857. The geography type is not
projected, and the default is SRID 4326, which uses lat/lng
coordinates. 

SRID support is heavily inbuilt into the SQL type system (see SQL
Types/Column Definitions).


#### spatial_ref_sys

Upon loading the PostGIS extension, a spatial_ref_sys table is
imported into the public schema of the given database, with a default
set of SRIDs pre-loaded into the database. This table exists in line
with the OGC Spec, with the addition of a `proj4text` column which
defines the WKT projection which has been transformed into something
the [PROJ](https://proj.org/) library can use. This table is similarly
insertable and deletable. Any Postgres user can modify this table.

PostGIS INSERT statements for custom defined SRIDs are available from
entries on the[ Spatial Reference
website](https://spatialreference.org/). If users are defining custom
SRIDs with WKT but do not have access to proj4text, we can provide a
builtin CGO wrapper around the [GDAL](https://gdal.org/) library
(X/MIT) which can perform this translation - but there are external
cli tools already available to perform this operation.

We need to be able to support some notion of a spatial_ref_sys table,
which allows management of user-defined insertable spatial_ref_sys
data.

For v20.2, we will not be looking to support custom SRIDs, and as such
SRIDs will be a hardcoded map and viewable from a `pg_extension`
virtual schema, discussed below in the "`pg_extension` schema" section.

In future versions, we can look at supporting user-defined
`spatial_ref_sys` schemas by allowing the user to "copy" the
`spatial_ref_sys` table into the public schema and using that
as the source of truth, analogous to PostGIS. This will appear
as the following:

```
$ CREATE EXTENSION 'postgis';
-- copy over spatial_ref_sys table, maybe even geometry_column/geography_column views
$ SELECT * FROM spatial_ref_sys
-- .... results copied from default spatial_ref_sys
$ INSERT INTO geom_table VALUES ('SRID=9999;POINT(1.0 1.0)')
-- lookup is based on what is available on the public schema as opposed
-- to a hardcoded map.
```

#### geometry_columns / geography_columns

The `geometry_columns` and `geography_columns` views in PostGIS are a
wrapper around pg_catalog which allow the user to view which tables
currently involve geospatial data. Due to PostGIS's extension nature,
they are available on the public schema upon extension registration.

This will also be available on the `pg_extension` schema
mentioned below.

##### `pg_extension` schema

For cross compatibility with PostGIS and its nature as an extension
which injects tables into the public schema, we need this table to be
resolvable from anywhere by simply requesting "geometry_columns",
"geography_columns" or "spatial_ref_sys".

For v20.2, this will be available on a virtual schema called `pg_extension`.
This will be on the search_path similar to how pg_catalog is today. This
will differentiate these views from the default virtual schema views 
available on other tables.

This schema should also be resolvable from any search path, and
thus available from any catalog. Any custom defined schemas should not
be named `pg_extension`.


#### spatial_ref_sys lookup caching

New column definitions (e.g. `ADD COLUMN &lt;colname> geography(point,
4326)`) as well as importing new geospatial datums with SRIDs defined
(e.g. `ST_FromEWKT('SRID=4326;POINT(1.0 2.0)')`) require a lookup of
whether the SRID is valid. It does not need to apply the
transformations, as it is expected that the input is already defined
as the given SRID.

To speed up having to look at the spatial_ref_sys for valid SRIDs, we
will need to cache successful SRID lookups. We can keep an
(potentially bounded) in-memory mapping of valid SRIDs with a
"timeout" to assist with the caching. This will be stale upon deletion
by an admin user, which the timeout will look to resolve.


#### ST_Transform

ST_Transform is the builtin that projects a given geospatial datum
from one SRID projection to another. This can be done as a CGO wrapper
around the [PROJ](https://proj.org/) library by transforming from one
proj4text to another. The library has a lot of extra additions that
may be unnecessary for our use case (e.g. requiring the installation of
sqlite3 for storing metadata from certain tools we don't use) and as
such we may decide to also ship this as a library which requires
dlopen/dlsym.

It is worth noting there is a Go port of the PROJ library named
[go-spatial/proj](https://github.com/go-spatial/proj), but is missing
some essential functionality such as the ability to define custom
proj4 transformations.


### Other Supported Operations


#### AddGeomColumn

This is a builtin provided by PostGIS that adds a new Geometry column
to the database given a set of parameters. This is used a lot by tools
that import geometry data (at least from tutorials), so we should look
to support the same builtin.

The AddGeomColumn primitive also adds a couple of CHECK expressions
that ensures that the SRIDs/shape in the column match the datums,
however, this is expected to work even when using the vanilla `ADD
COLUMN geometry(shape, srid)` syntax. This is probably a relic of old
PostGIS, and we will look to add the same CHECK expressions.


### BACKUP/RESTORE/cockroach dump

Backup/Restore/cockroach dump operations will continue to "work out of
the box". We will furthermore be able to ingest postgres dumps of
PostGIS data as we support I/O of EWKT-as-hex format when parsing and
displaying geospatial datums.


### CHANGEFEED

We will use EWKB for CHANGEFEED results for Geography and Geometry
types. This may be out of line with the OGC spec, but follows the
principle that we will prioritize PostGIS behaviour.


### Out of Scope for v20.2

The following has been mentioned as out of scope:
* 2.5D, 3D shapes
* PRIMARY KEY definitions for Geometry/Geography types
* Default Arguments for Builtins
* Caching certain builtin operations
* User defined SRIDs

Furthermore, these will be out of scope:
* Supporting the default geometric data types in
  [PostgreSQL](https://www.postgresql.org/docs/current/datatype-geometric.html).


## Indexing


### Background

The current approaches to geospatial indexes divide cleanly into two
buckets. One approach is to "divide the objects". This works by
inserting the objects into a tree (usually a balanced tree) whose
shape depends on the data being indexed. The other approach is to
"divide the space". This works by creating a decomposition of the
space being indexed into buckets of various sizes.

When an object is indexed, a "covering" shape (e.g. a bounding box) is
constructed that completely encompasses the indexed object. Index
queries work by looking for containment or intersection between the
covering shape for the query object and the indexed covering
shapes. This retrieves false positives but no false negatives.


#### Divide the Objects

PostGIS is the notable implementation of divide the objects. It
maintains an "R tree" (rectangle tree) which is implemented as a
Postgres "GiST" index. The GiST index is a generalization of data
types that can be naturally ordered into a hierarchy of supersets
(e.g.B+ trees, R trees, etc).

The covering shape used by PostGIS is a "bounding box" which is the
minimal rectangle that encompasses the indexed shape. The same is done
for the query shape.

Lucene uses [BKD
trees](https://users.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf)
with a triangle tessellation of shapes. BKD trees permit a
log-structured multi file approach, but compactions would need to read
all the input files into memory to redivide the objects. More
importantly, they are not easily compatible with the normal horizontal
scaling approach of splitting and merging lexicographic ranges.

Advantages of Divide the Objects:

* Each object is present in the index once
* Can index 3D GEOMETRY space
* Can index GEOGRAPHY with altitude
* Can index infinite GEOMETRY space

Disadvantages of Divide the Objects:

* The tree needs a periodic balancing operation to have predictable
  read latencies
* Object insertions and balancing require locking
* How to effectively horizontally distribute an R tree is an [open
  question](https://www.anand-iyer.com/papers/sift-socc2017.pdf).
* Bulk ingest (IMPORT) requires coordination between nodes as the
  tree's shape depends on its contents
* A bounding box can return many false positives

#### Divide the Space

Recent geospatial index implementations tend to prefer to divide the
space, because being able to horizontally distribute data is
increasingly important. The relevant implementations here are
Microsoft SQL Server and MongoDB.

The space is divided into a quadtree (or a set of quadtrees) with a
set number of levels and a data-independent shape. Each node in the
quad tree (a "cell") represents some part of the indexed space and is
divided once horizontally and once vertically to produce 4 children in
the next level. Each node in the quadtree is "content addressable",
meaning that mapping from the node to its unique ID and back is
possible without external information.

Implementations tend to use clever strategies for the unique IDs with
important guarantees:

* The IDs of all ancestors of a cell are enumerable
* The IDs of all descendants of a cell is a range query
* The cells of nearby IDs are spatially near

MongoDB uses the [S2 library](https://s2geometry.io/) from Google for
Geography and their own implementation for 2D planar geometry. The
latter uses a
[geohash](http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves)
based numbering of cells which has worse locality than the Hilbert
curve used by S2. SQL Server uses an implementation of their own
devising.

When indexing an object, a covering is computed, often using some
number of the predefined cells. The same is done for the query object
when using the index. Ancestors and descendants can be retrieved by
using the ID properties above.

The number of covering cells can vary per indexed object. There is an
important tradeoff in the number of cells used to represent an object
in the index: fewer cells use less space but create a looser
covering. A looser covering retrieves more false positives from the
index, which is expensive because the exact answer computation that's
run after the index query is expensive. However, at some point the
benefits of retrieving fewer false positives is outweighed by how long
it takes to scan a large index.

Because the space is divided beforehand, it must be finite. This means
that Divide the Space works for (spherical) GEOGRAPHY and for finite
GEOMETRY (planar) but not for infinite GEOMETRY. SQL Server requires
that finite bounds be declared when creating a GEOMETRY index.

Current divide the space approaches (both S2 and SQL Server) assume
flat space, so cannot be used for indexing 3D GEOMETRY or altitude
GEOGRAPHY. Conceptually, the approach is extendable to 3D, but would
require our own implementation, and the performance characteristics of
indexing a larger space in this manner are unknown.

Advantages of Divide the Space:

* Easy to scale horizontally
* No balancing
* Inserts require no locking
* Bulk ingest is simple
* Allow a per-object tradeoff between index size and false positives

Disadvantages of Divide the Space:

* Do not easily support indexing infinite GEOMETRY (but we discuss a
  way to extend to infinite geometries in future work).
* Performance characteristics of indexing 3D GEOMETRY space and
  indexing altitude GEOGRAPHY are unknown.


### Approach

For geography we will use the S2 library that models a sphere using a
quad-tree divide-the-space approach and numbers the cells using a
Hilbert Curve. The cell ids are 64 bit integers and the leaf cells
measure 1cm across on the Earth’s surface. For 2D planar geometry, we
will initially reuse the S2 library by mapping the rectangular bounds
of the index to one face of the unit cube in S2, and then mapping it
to the unit sphere. This distorts the original shape, but index
lookups (discussed in more detail below) look for cell overlaps, and
overlaps are preserved by this distortion.

In future versions we may write our own divide-the-space library for
2D planar geometry that avoids any distortions, and would permit
removal of the rectangular index bounds (see future work section).


### Index storage

An index is configured with parameters that control aspects of the
covering produced for a shape: the number of cells, the lowest and
highest levels of the cells. The cell-ids are represented as
uint64. The stored index is an inverted index consisting of this
cell-id column followed by the primary key columns in the indexed
table. Note that each cell-id can be used in the covering for multiple
table rows, and each table row can have multiple cell-ids in the
covering, that is, it is a many-to-many relationship.

The following picture shows a nyc census block in purple with a 4 cell
covering in teal. Note that the cells are of different sizes. The
cell-id for one of the cells in the covering is also shown. 4 cells
were used in this case since that was the maximum number of cells
configured in the index configuration, stored in the IndexDescriptor.

![Census Block Covering](20200421_geospatial/census_block1.png?raw=true "Census Block Covering")

This on-disk representation of the inverted index splits the “posting
list” for each cell-id, where the list is the table rows that are
indexed by that cell-id, into one index row per list element. This is
simple but not very efficient for reads, since each index row is
potentially small and queries always need to read all the rows for a
cell-id. However, it is convenient for additions and deletions, since
a single row needs to be modified. Preliminary experiments indicate
that the simple approach can be competitive in performance with
PostGIS, so we will adopt this simple approach until otherwise
necessary.


### Index queries

We first consider how indexes are used to accelerate filtering and
then extend it to join queries.


#### Filtering

Consider a boolean function op(g, x) where g is a given shape, and x
is an indexed shape for which this function is true. The index is used
to find the set of all x for which this function could return
true. All functions are mapped to the following for index
acceleration:

* `contains(g, x)`: Every point in shape x is also in g, i.e., g
  contains x.
* `contained-by(g, x)`: This is equivalent to contains(x, g), but is
  considered separately since x represents the shapes that are
  indexed.
* `intersects(g, x)` or `intersects(x, g)`: g and x have at least 1 point
  that is in both.

Consider the covering(s) for any shape s to be the set of cellids that
cover the shape s. PostGIS uses bounding boxes instead of cell
coverings. Bounding boxes have the following property:

* contains(s1, s2) => contains(bounding-box(s1), bounding-box(s2))
* intersects(s1, s2) => intersects(bounding-box(s1), bounding-box(s2))

We assume the same property for cell coverings:

* contains(s1, s2) => contains(covering(s1), covering(s2))
* intersects(s1, s2) => intersects(covering(s1), covering(s2))

The first property is not always true and we will discuss how to
adjust to that reality in a later section.

The following abstract example is used to illustrate the filtering
algorithm, where c[i] is a cell number. Consider g has the cell
covering c[213], c[61], c[64] in a quad-tree rooted at c[0]. For
convenience of illustration, the numbering is not the Hilbert curve
numbering used by S2. In the numbering here, the children of cell c[i]
are numbered c[4*i+1]...c[4*i+4]. The following depicts this covering
as a tree with the leaf cells being the covering cells. Note that the
paths from the leaves to the root are not the same length.


```
                     c[0]
                      |
                     c[3]
                      |
                  +---+-----+
                  |         |
                c[13]     c[15]
                  |         |
                c[53]    +--+--+
                  |      |     |
               C[213]  c[61] c[64]

```



* contains(g, x): Using the covering property from earlier, all shapes
  contained by g must have coverings contained by {c[213], c[61],
  c[64]}. In the full quad-tree (not the partial one depicted above),
  these are all the shapes indexed in the subtrees rooted at c[213],
  c[61], c[64]. Due to the locality of the Hilbert curve numbering,
  each subtree is a single contiguous range of integers. The indexed
  shapes that could satisfy this function are:

  ```
    (⋃ \for-all c in subtree-range(c[213]) index(c)) ⋃
    (⋃ \for-all c in subtree-range(c[61]) index(c)) ⋃
    (⋃ \for-all c in subtree-range(c[64]) index(c))
  ```  

    There can be duplicate shapes both within a subtree and across
    subtrees. For example, a shape could be indexed under two children
    of c[61], say c[245] and c[247]. The scan of the subtree rooted at
    c[61] will find it twice.

* contained-by(g, x): Using the covering property from earlier, all
  shapes containing g must have coverings that contain c[213] and
  contain c[61] and contain c[64]. The indexed shapes that could
  satisfy this function are

  ```
    (index(c[213]) ⋃ index(c[53]) ⋃ index(c[13]) ⋃ index(c[3]) ⋃ index(c[0])) ⋂
    (index(c61) ⋃ index(c15) ⋃ index(c3) ⋃ index(c0)) ⋂
    (index(c64) ⋃ index(c15) ⋃ index(c3) ⋃ index(c0))
  ```
  
  One can factor out common sub-expressions.

* intersects(g, x) (or (x, g)): This needs to retrieve the shapes that
  satisfy contains(g, x) and shapes indexed using the ancestors of
  c213, c61, c64. That is,

  ```
    contains(g, x) ⋃ index(c0) ⋃ index(c3) ⋃ index(c13) ⋃ index(c53) ⋃ index(c15)
  ```


#### Mapping to the index functions

Functions map to the index functions:

* ST_Covers(g, x), ST_Covers(x, g): use contains(g, x) or
  contained-by(g, x)
* ST_CoveredBy(g, x), ST_CoveredBy(x, g): use contained-by(g, x) or
  contains(g, x)
* ST_Contains(g, x), ST_Contains(x, g): use contains(g, x) or
  contained-by(g, x)
* ST_ContainsProperly(g, x), ST_ContainsProperly(x, g):  use contains(g, x) or
  contained-by(g, x)
* ST_Crosses: use intersects
* ST_DFullyWithin(g, x, d), ST_DFullyWithin(x, g, d): extend g by
  distance d to produce a shape g’, and then use contains(g', x). The
  S2 library has an S2ShapeIndexBufferedRegion class that can be used
  for extending by distance d for the Geography type. This is not part
  of the S2 Go library, so we will need to port it from C++. See a
  later section for the Geometry type.
* ST_DWithin(g, x, d), ST_DWithin(x, g, d): extend g by distance to
  produce a shape g’, and then use intersects.
* ST_Equals: use intersects (we may be able to do better later) 
* ST_Intersects: use intersects
* ST_Overlaps: use intersects
* ST_Touches: use intersects
* ST_Within(g, x), ST_Within(x, g): use contained-by(g, x) or contains(g, x)


#### Joins

Queries can join between tables with geospatial indexes such as

```
SELECT blocks.blkid
FROM nyc_census_blocks blocks
JOIN nyc_subway_stations subways
ON ST_Contains(blocks.geom, subways.geom)
```

We cannot execute this by joining between two inverted indexes. This
is no different from PostGIS, which does not attempt to join two
R-tree indexes. This query will execute using a special geospatial
inverted-index “lookup join”:

* Each table row of one of the tables (typically the smaller one)
  produces the given shape g (from the previous section).
* The covering of g is computed, and is used to lookup cell-ids (and
  cell-id ranges) from the index of the other table.
* The set expression (described in the previous section) is computed
  to generate the matching shapes.

We plan to generalize this geospatial “lookup join” to also be usable
for other inverted index cases, like ARRAY and JSON.


#### Query Planner/Optimizer

The optimizer will need to perform three major tasks to support
geospatial queries:

* _Detect functions with constant inputs and perform constant
  folding_.

  * Since we already support constant folding for functions, this
    should be as simple as adding the relevant function names to the
    `FoldFunctionWhitelist`.

* _Detect index accelerated functions (see the list above) in the
  WHERE clause with one constant and one non-constant input, and
  generate constrained index scans_.

  * This will be an exploration rule which detects primary index scans
    wrapped in a `SELECT` with predicate `WHERE ST_Covers(g, x)` (or
    any other index accelerated function in which `g` is constant and
    there is an index on `x`). The rule will create an alternative
    plan in which the primary index scan is replaced with an
    expression containing constrained inverted index scans combined
    with set operations. The specific index constraints and set
    operators (`UNION` or `INTERSECT`) will depend on the values
    returned by the `contains`, `contained-by` or `intersects` APIs
    described above. Later on, we can make these set expressions more
    efficient using other transformation rules (e.g., convert the
    `UNION`s to `UNION ALL` + `DISTINCT`) or help from the execution
    engine (e.g., to support scanning multiple inverted index ranges
    as part of a single scan). Note that the `SELECT` operator must
    still remain after the set expressions to filter out false
    positives returned by the index scans.

  * In order for the optimizer to decide between the original primary
    index scan, an alternate secondary index scan (e.g., if there is
    another predicate besides `ST_Covers`), or the set expression with
    inverted index scans, we will need to update the cost model and
    statistics code. For the cost model, we will need to understand
    how expensive the predicate `ST_Covers` is compared to the cost of
    scanning a row. For the statistics code, we will need an estimate
    of how much data must be scanned for each of the inverted index
    scans, as well as how many rows are returned by the full set
    expression. The statistics code for inverted indexes is currently
    very rudimentary, so this should be improved whilst benefitting
    JSONB and ARRAY.

  * To improve the statistics code for inverted indexes, there are a
    couple of options: (1) Store the number of distinct keys in the
    index, as well as the total number of values in the index
    (counting duplicates). To estimate the selectivity of an equality
    predicate (e.g., x = 5), we would use the formula (1/no. distinct
    keys) * (no. values in the index/no. of rows). (2) A more accurate
    selectivity estimate would use a histogram, storing the number of
    values indexed by each key. This also includes duplicate entries,
    so the total number of values in the histogram would add up to
    more than the number of rows. Making these improvements to
    inverted index statistics would not only help support geospatial
    queries, but also improve our JSON support.
  
* _Detect index accelerated functions (see the list above) in the
  WHERE/ON clause with two non-constant inputs, and generate lookup
  joins_.

  * Similar to the previous case, this will be an exploration rule
    which detects primary index scans wrapped in a predicate
    containing one of the functions that can be
    index-accelerated. However, this rule will only apply if both
    inputs to the function are variables, and at least one has an
    inverted index. In this case, the optimizer will generate one or
    more lookup joins, in which one of the variables is used to look
    up into the index of the other variable. As with the case above,
    the predicate must remain after the join to filter out false
    positives.

  * To decide which index to use for the join (or whether to use one
    of the indexes at all), we will need to make the same changes to
    the cost model and statistics code described above. We will also
    need an estimate of how expensive the API calls to `contains`,
    `contained-by` or `intersects` are since they will need to be
    called on each input row. Additionally, we will need to improve
    the selectivity calculation for lookup joins in cases where the
    index is inverted.

    Since the set operations required for each row make selectivity
    calculation on geospatial lookup joins especially difficult, we
    may consider adding a new type of histogram for geospatial data.
    The histogram buckets would be ranges of S2 cell IDs, and the
    counts would represent the number of objects in the geospatial
    column that overlap that cell. By joining histograms on two
    geospatial columns, we could estimate the selectivity of a real
    join on those columns

### Weakness of Covering invariant

The previous sections have assumed the invariant


```
contains(s1, s2) => contains(covering(s1), covering(s2))
```


This is not true in the general case even if the parameters used for
computing the covering are the same:

* It *may* be true due to implementation artifacts of the S2 library
  for the shapes we are using, but this needs very careful
  verification. Specifically, for coverings produced using the same
  settings, the region coverer code in S2 may achieve this because of
  how it prioritizes which [cells to
  expand](https://sourcegraph.com/github.com/google/s2geometry/-/blob/src/s2/s2region_coverer.cc#L187-192)
  (using level and number of children). But the MayIntersect
  computation used is allowed to have [false
  positives](https://sourcegraph.com/github.com/google/s2geometry/-/blob/src/s2/s2region.h#L85-92),
  though for the shapes we are using it claims not to have false
  positives.

* We have experimentally observed benefits in using more cells in the
  covering of the query shape (the g parameter). One way to work
  around this would be to use a finer cell covering for g for
  contained-by(g, x) and one that matches the index for contains(g,
  x).


![Covering Invariant](20200421_geospatial/covering_invariant.png?raw=true "Covering Invariant")

The illustration above shows an example: the blue shape has a covering
represented by the two blue cells and the orange shape has a covering
represented by the larger orange cell (which is the parent of the blue
cells).

To begin with, we will work around this as follows:

* For contained-by(g, x), use an “inner” covering for g. An inner
  covering is analogous to an interior covering, i.e., cells that are
  fully contained in g, but can additionally use leaf level cells that
  overlap with g.
* Loosen the index computation for contains(g, x) to be identical to
  intersects. This means a very small g, which is contained by many
  large shapes will have an index lookup that will retrieve all these
  large shapes that will be false positives.


#### Alternatives to Inner covering

If we can’t strengthen the covering invariant another alternative
would be to store both the regular covering and the inner covering in
the inverted index. Cells that are in both coverings (e.g. a point
shape will have identical inner and regular covering) would need to be
written once and marked as part of both coverings.


### Considerations for 2D Planar Geometry

We will use S2 to index 2D planar geometry by mapping the rectangular
bounds of the index to the unit square, which is then mapped to the
unit cube. We consider the potential issues with this approach:

* Shapes that exceed the index bounds: we clip the shape to the index
  bounds and index the clipped shape in the usual
  manner. Additionally, the shape will be indexed under a special
  “spillover” cell-id (that is not used by S2). Queries where the
  specified shape exceeds the bounds will need to read this spillover
  cell-id in addition to reading the rest of the index using the
  clipped shape.

* The mapping of the rectangular bounds to the unit square and then
  mapping to the unit sphere introduces distortions. This are ok for
  most operations since both the query shape and the indexed shape
  will continue to share the same cells (where they overlap) despite
  the distortion. However this an issue for ST_DWithin and
  ST_DFullyWithin which additionally specify a distance: the mapping
  to the unit sphere uses a non-linear function so the distance on the
  unit cube face translates differently based on where it is located
  on the cube. Instead of using S2 to extend the shape by distance (as
  we do for Geography), we will extend the shape by distance using the
  `GEOSBuffer_r` function in the GEOS library, and then translate the
  returned geometry to S2.


### Future Work

Indexing is an open-ended area and there is much potential for future
improvements. We will prioritize improvements based on user
experience, so it is hard to know what we will discover. Here are some
early ideas on where we could work on improvements:

* K nearest neighbor search: This is a hard problem with a
  divide-the-space approach. We have some preliminary ideas for how to
  support this based on distances between the centroids of the
  bounding box, but this will wait until we have more clarity on user
  requirements.

* Eliminate the bounding box in 2D planar indexes: This will
  potentially go together with replacing our use of S2 for 2D planar
  geometries with our own divide-the-space approach (the challenge
  here is fast and robust algorithms for determining containment and
  intersection of a cell with a shape). We have some early ideas on
  how one can use a variable number of bits in the cell-id, and a 4
  quadrant plane, and extend the 4 quadrants indefinitely starting
  from an origin. This approach requires a different key comparator
  than the current MVCC comparator for the geospatial
  index. Additionally, it may require some storage engine hooks to
  rewrite keys as part of compactions.

* More efficient storage and retrieval of inverted index: Storing and
  retrieving an inverted index with one row per (cell-id, shape) is
  inefficient. The typical alternative is to use posting lists of the
  form cell-id => set of shape ids, where the shape ids are numeric
  and dense and can be packed efficiently using delta and run-length
  encoding. This allows for efficient posting list union and
  intersection (e.g. using roaring bitmaps). A big issue in an OLTP
  setting is insertions and deletions to the set of shape ids. Reading
  and rewriting the set for each update will introduce a hotspot in
  the key space. An alternative we could explore is to use storage
  engine level merges to incorporate changes to the set. This cannot
  be done without storage engine changes since the engine would need
  to know about (a) what is not committed, since uncommitted changes
  should not be merged with committed changes, (b) merge across
  different MVCC timestamps, (c) split long posting lists into
  multiple key-value pairs. Some of these issues are common to what
  would be needed to do GC of old MVCC versions within the engine (as
  part of compactions). We do not expect this to happen in the near
  future.

* Working with the default geometric data types in
  [PostgreSQL](https://www.postgresql.org/docs/current/datatype-geometric.html),
  in particular, [the
  indexing](https://www.postgresql.org/docs/current/indexes-types.html).


## End-to-End Example

We consider the following query adapted from
[https://postgis.net/workshops/postgis-intro/joins_exercises.html](https://postgis.net/workshops/postgis-intro/joins_exercises.html).


### Table Definitions

Consider the following table definitions:

<table>
  <tr>
   <td>-- copied from output of shp2pgsql
<p>
CREATE TABLE "nyc_census_blocks" (gid serial,
<p>
"blkid" varchar(15),
<p>
"popn_total" float8,
<p>
"popn_white" float8,
<p>
"popn_black" float8,
<p>
"popn_nativ" float8,
<p>
"popn_asian" float8,
<p>
"popn_other" float8,
<p>
"boroname" varchar(32));
<p>
ALTER TABLE "nyc_census_blocks" ADD PRIMARY KEY (gid);
<p>
SELECT AddGeometryColumn('','nyc_census_blocks','geom','0','MULTIPOLYGON',2);
<p>
-- create an index
<p>
CREATE INDEX nyc_census_blocks_geo_idx ON nyc_census_blocks USING GIST(geom);
<p>
-- and add 38794 rows
   </td>
  </tr>
  <tr>
   <td>-- copied from output of shp2pgsql
<p>
CREATE TABLE "nyc_neighborhoods" (gid serial,
<p>
"boroname" varchar(43),
<p>
"name" varchar(64));
<p>
ALTER TABLE "nyc_neighborhoods" ADD PRIMARY KEY (gid);
<p>
SELECT AddGeometryColumn('','nyc_neighborhoods','geom','0','MULTIPOLYGON',2);
<p>
-- create an index
<p>
CREATE INDEX nyc_neighbourhoods_geo_idx ON nyc_neighbourhoods USING GIST(geom);
<p>
-- and add 129 rows
   </td>
  </tr>
</table>


Now we run the following query:


```
SELECT
  n.name,
  Sum(c.popn_total) / (ST_Area(n.geom) / 1000000.0) AS popn_per_sqkm
FROM nyc_census_blocks AS c
JOIN nyc_neighborhoods AS n
ON ST_Intersects(c.geom, n.geom)  AND c.boroname = n.boroname
WHERE n.name = 'Upper West Side'
OR n.name = 'Upper East Side'
GROUP BY n.name, n.geom;
```

Note both `nyc_census_blocks` and `nyc_neighborhoods` have geospatial
indexes on the geom column.


### Example Match with S2 Coverings

Let's take an example match from the above query and look at the S2
generated index coverings.

Census Block 360610138001000 is in purple, with a generated S2
covering coloured in teal. The covering completely highlights the
entire census area using a maximum of 4 cells (which we have specified
experimentally for the sake of this example).


![Census Block Covering](20200421_geospatial/census_block2.png?raw=true "Census Block Covering")
![Census Block Covering with Cells](20200421_geospatial/census_block3.png?raw=true "Census Block Covering with Cells")

Now here is the Upper East Side, with S2 using a single cell as a
cover for it. Again, we have experimentally set the use of a maximum
of 4 cells, but S2 was happy just using the one.


![Upper East Side](20200421_geospatial/ues1.png?raw=true "Upper East Side")
![Upper East Side With Cells](20200421_geospatial/ues2.png?raw=true "Upper East Side With Cells")


Here is everything so far overlayed with each other. Here we can
explicitly see that the coverings of the Census Block are (some unit
of grand) children of the Upper East Side, and there is a clear
intersection of the Census Block and the Upper East Side.

![Upper East Side With Census Block](20200421_geospatial/ues_with_census_block.png?raw=true "Upper East Side With Census Block")

Let us consider trying to see whether we should try and match them
together with S2 coverings.

Consider the CellIDs in binary format:


```
UES:  1000100111000010010110001100000000000000000000000000000000000000
Blk1: 1000100111000010010110001011110001000000000000000000000000000000
Blk2: 1000100111000010010110001011101110001010110000000000000000000000
Blk3: 1000100111000010010110001011101110001011010000000000000000000000
Blk4: 1000100111000010010110001011101111110000000000000000000000000000
```

```
             ^-----------------------------------------^
```

With the S2 encoding mechanism, we can tell which items
intersect with the single cell covering of the Upper East Side by
considering the range

```
[1000100111000010010110001000000000000000000000000000000000000000,1000100111000010010110010000000000000000000000000000000000000000)
```

as well as any parent coverings by constantly
1000100111000010010110001100000000000000000000000000000000000000
dividing by 4 until we have reached the root sub-tree. This would
match all 4 blocks as indicated above.


### Planning

The number of neighborhoods is 129 and the number of census blocks is
38794. Even though name is not the primary key of the
`nyc_neighborhoods` table, the cardinality of the name field is the
same, 129. So the optimizer can predict that the name filter will
match 2 rows. This filter will be pushed below the join, so the input
to the join from `nyc_neighborhoods` is 2 rows.

The optimizer will consider several different types of joins when
joining `nyc_neighborhoods` and `nyc_census_blocks`. The two most
likely options are (1) a hash join on the predicate `c.boroname =
n.boroname`, and (2) a geospatial lookup join on the predicate
`ST_Intersects(c.geom, n.geom)`, using the inverted index on
`nyc_census_blocks`.

In order to decide between these two options, the optimizer can use
statistics. The boronames have cardinality 5, so the number of rows
returned by the hash join is 38794*2/5 = 15518. The expensive
`ST_Intersects(c.geom, n.geom)` function must then be called on each
of those rows.

The cost of the geospatial lookup join is more challenging to
estimate, but it can be calculated by first estimating the number of
CellIDs that must be scanned in `nyc_census_blocks_geo_idx` for each
of the two matching rows in `nyc_neighborhoods`. Suppose that
approximately 60 CellIDs must be scanned for a particular row, and the
cardinality of the inverted index (i.e., number of CellIDs with
entries) is 6000. Since each census block will exist in 4 index
entries, the total number of values indexed is 38794*4 =
155176. Therefore, the number of entries scanned is (60/6000)*155176*2
= 3104. After de-duplicating the results, the number of output rows is
3104/4 = 776. The expensive `ST_Intersects(c.geom, n.geom)` function
must then be called on each of those rows to eliminate false
positives, and the inexpensive predicate `c.boroname = n.boroname`
must also be applied.

Based on the above cost analysis, the optimizer decides to use the
geospatial index to do a geospatial-lookup join, where each of the
selected rows from the `nyc_neighborhoods` table will be used to
construct and compute a set union expression on the inverted
geospatial index of `nyc_census_blocks`.

Here is the expected query plan:


```
QUERY PLAN                                                      
--------------------------------------------------------------------------------------
project
 ├── group-by
 │    ├── inner-join (lookup nyc_census_blocks AS c)
 │    │    ├── lookup columns are key
 │    │    ├── inner-join (geospatial-lookup nyc_census_blocks@nyc_census_blocks_geo_idx)
 │    │    │    ├── select
 │    │    │    │    ├── scan nyc_neighborhoods AS n
 │    │    │    │    └── filters
 │    │    │    │         └── (name = 'Upper West Side') OR (name = 'Upper East Side')
 │    │    │    └── filters (true)
 │    │    └── filters
 │    │         ├── st_intersects(c.geom, n.geom)
 │    │         └── c.boroname = n.boroname
 │    └── aggregations
 │         └── sum
 │              └── popn_total
 └── projections
      └── sum / (st_area(n.geom) / 1e+06)
```



### Execution

This query will execute in the manner outlined by the query plan:



* Since there is no index on nyc_neighborhoods.name, the
  nyc_neighborhoods table is scanned and the name filter is
  applied. For each of the two rows that pass the filter, the
  following steps will be performed.

* A covering of the geom field in the row in nyc_neighborhoods is
  computed. This covering is used by the intersects() function
  outlined in the previous [section](#filtering), to produce a set of
  cell-ids that must be retrieved from the index and the corresponding
  nyc_census_blocks.gid column, which is the primary key,
  unioned. This is the candidate set of census_blocks keys that
  satisfy ST_Intersects.

* The primary key of nyc_census_blocks (nyc_census_blocks.gid) is then
  used to perform a lookup join with the primary index of
  nyc_census_blocks, which is needed to retrieve the remaining
  columns. As part of this lookup join, the ST_Intersects predicate
  and the boroname equality predicate are computed and if both are
  true, the join condition is satisfied.

* Next, the sum of the total population is calculated, grouped by
  neighborhood.

* Finally, the sum is divided by the area of the neighborhood, in
  order to find the population per square meter. Area is calculated
  using the built-in scalar function ST_Area.


## Package Structure

We will put geospatial calculation data in a new `pkg/geo` package, to
separate "SQL" from "Geospatial". We've decided to separate
"functions" and their complexity into subpackages to simplify import
complexity.

Our package structure will look as follows:

* pkg
  * geo (contains basic data types, as well as transformations from/to
    external data formats)
    * geopb (contains protobuf storage for shapes, as well as typedefs
      for SRIDs)
    * geogfn (contains geography functions, as well as the CGO
      wrappers for GeographicLib)
    * geomfn (contains geometry functions, as well as the CGO wrappers
      for GEOS / SFCGAL)
    * geosrid (contains SRID transformation functionality)
    * geoindex (contains indexing functionality)
    * geographiclib (contains spheroid functionality from
      GeographicLib)
    * geos (contains 2D geometry from the GEOS library)
    * geoproj (contains proj functionality from PROJ)

## Telemetry

To gather insight, we should add telemetry on the following:

* SQL Usage
  * column type usage
  * usage statistics of various builtins
  * success rate of caching SRID
  * success rate of caching builtins (if implemented)
* Indexing
  * spatial index usage
  * spatial index using out of bounds geometries
  * filtering % done by using indexes

## Testing

In addition to writing our own unit and integration tests, there are
several third-party tools we can look at utilizing to compare
correctness. These can be adapted to and used in the CockroachDB
framework, including:

* [OGC Conformance Tests](https://cite.opengeospatial.org/teamengine/)
  - tests to ensure OGC compatibility - adaptable into nightly
  roachtests
* [monetdb-mcs](https://github.com/cswxu/monetdb-mcs/tree/544d87e9e67a926e88b17b171aa4251b20177be0/geom/)
  has a few of their own integration tests - adaptable into nightly
  roachtests

Due to internal restrictions, we will be unable to utilize
the following directly from the codebase:

* [PostGIS regression
  tests](https://github.com/postgis/postgis/blob/master/regress )

* [PostGIS "garden"
  tests](https://trac.osgeo.org/postgis/wiki/DevWikiGardenTest) -
  tests for crashing / running into edge cases. We can utilize
  sqlsmith to write a similar version for ourselves.

We will also look at correctness testing our own index
implementation. We can utilize the default GEOS and GeographicLib
calculations to determine whether our index implementation will
correctly fetch the same results as without using the index. To do
this, we can generate random test suites with indexed columns and
compare results with and without using the index. As an added bonus,
we can see the performance improvement when using indexing as well
with this approach.


## Performance Benchmarking

At the time of writing, we have struggled to find industry standard
benchmarks for comparing performance of spatial indexes. As such, we
are mainly left to our own devices.

We will be looking at writing our own benchmarking suite, which will
benchmark on the following:

* Various core geospatial builtins
* Indexed retrievals
  * We should test indexed retrievals against numbers of rows, as well
    as different sizes of geospatial shapes (e.g. lots of little
    and/or big shapes) as well as sparseness of geospatial shapes
    (e.g. lots of data in "New York" vs data sparsely populated around
    different areas).
  * We should also try to test under lock contention (to test the
    theory that R-Trees do not behave well under conditions involving
    heavy locking)
* IMPORT/EXPORT functionality

We may decide to compare our implementation against other SQL-based
implementations such as SQL Server and Postgres/PostGIS. MongoDB is a
good stretch option as well.

## Unresolved questions

None beyond what is already mentioned in earlier text.

# Updates
* 2020-05-07:
  * added ST_ContainsProperly as an indexable function.
* 2020-08-17:
  * update install instructions
  * update bbox notes
  * update index specification
  * added notes on ogr2ogr
