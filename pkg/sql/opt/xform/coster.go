// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/tools/container/intsets"
)

// Coster is used by the optimizer to assign a cost to a candidate expression
// that can provide a set of required physical properties. If a candidate
// expression has a lower cost than any other expression in the memo group, then
// it becomes the new best expression for the group.
//
// The set of costing formulas maintained by the coster for the set of all
// operators constitute the "cost model". A given cost model can be designed to
// maximize any optimization goal, such as:
//
//   1. Max aggregate cluster throughput (txns/sec across cluster)
//   2. Min transaction latency (time to commit txns)
//   3. Min latency to first row (time to get first row of txns)
//   4. Min memory usage
//   5. Some weighted combination of #1 - #4
//
// The cost model in this file targets #1 as the optimization goal. However,
// note that #2 is implicitly important to that goal, since overall cluster
// throughput will suffer if there are lots of pending transactions waiting on
// I/O.
//
// Coster is an interface so that different costing algorithms can be used by
// the optimizer. For example, the OptSteps command uses a custom coster that
// assigns infinite costs to some expressions in order to prevent them from
// being part of the lowest cost tree (for debugging purposes).
type Coster interface {
	// ComputeCost returns the estimated cost of executing the candidate
	// expression. The optimizer does not expect the cost to correspond to any
	// real-world metric, but does expect costs to be comparable to one another,
	// as well as summable.
	ComputeCost(candidate memo.RelExpr, required *physical.Required) memo.Cost
}

// coster encapsulates the default cost model for the optimizer. The coster
// assigns an estimated cost to each expression in the memo so that the
// optimizer can choose the lowest cost expression tree. The estimated cost is
// a best-effort approximation of the actual cost of execution, based on table
// and index statistics that are propagated throughout the logical expression
// tree.
type coster struct {
	mem *memo.Memo

	// locality gives the location of the current node as a set of user-defined
	// key/value pairs, ordered from most inclusive to least inclusive. If there
	// are no tiers, then the node's location is not known. Example:
	//
	//   [region=us,dc=east]
	//
	locality roachpb.Locality

	// perturbation indicates how much to randomly perturb the cost. It is used
	// to generate alternative plans for testing. For example, if perturbation is
	// 0.5, and the estimated cost of an expression is c, the cost returned by
	// ComputeCost will be in the range [c - 0.5 * c, c + 0.5 * c).
	perturbation float64
}

var _ Coster = &coster{}

// MakeDefaultCoster creates an instance of the default coster.
func MakeDefaultCoster(mem *memo.Memo) Coster {
	return &coster{mem: mem}
}

const (
	// These costs have been copied from the Postgres optimizer:
	// https://github.com/postgres/postgres/blob/master/src/include/optimizer/cost.h
	// TODO(rytaft): "How Good are Query Optimizers, Really?" says that the
	// PostgreSQL ratio between CPU and I/O is probably unrealistic in modern
	// systems since much of the data can be cached in memory. Consider
	// increasing the cpuCostFactor to account for this.
	cpuCostFactor    = 0.01
	seqIOCostFactor  = 1
	randIOCostFactor = 4

	// TODO(justin): make this more sophisticated.
	// lookupJoinRetrieveRowCost is the cost to retrieve a single row during a
	// lookup join.
	// See https://github.com/cockroachdb/cockroach/pull/35561 for the initial
	// justification for this constant.
	lookupJoinRetrieveRowCost = 2 * seqIOCostFactor

	// virtualScanTableDescriptorFetchCost is the cost to retrieve the table
	// descriptors when performing a virtual table scan.
	virtualScanTableDescriptorFetchCost = 25 * randIOCostFactor

	// Input rows to a join are processed in batches of this size.
	// See joinreader.go.
	joinReaderBatchSize = 100.0

	// latencyCostFactor represents the throughput impact of doing scans on an
	// index that may be remotely located in a different locality. If latencies
	// are higher, then overall cluster throughput will suffer somewhat, as there
	// will be more queries in memory blocking on I/O. The impact on throughput
	// is expected to be relatively low, so latencyCostFactor is set to a small
	// value. However, even a low value will cause the optimizer to prefer
	// indexes that are likely to be geographically closer, if they are otherwise
	// the same cost to access.
	// TODO(andyk): Need to do analysis to figure out right value and/or to come
	// up with better way to incorporate latency into the coster.
	latencyCostFactor = cpuCostFactor

	// hugeCost is used with expressions we want to avoid; these are expressions
	// that "violate" a hint like forcing a specific index or join algorithm.
	// If the final expression has this cost or larger, it means that there was no
	// plan that could satisfy the hints.
	hugeCost memo.Cost = 1e100

	// fullScanRowCountPenalty adds a penalty to full table scans. This is especially
	// useful for empty or very small tables, where we would get plans that are
	// surprising to users (like full scans instead of point lookups).
	fullScanRowCountPenalty = 10

	// unboundedMaxCardinalityScanRowCountPenalty adds a penalty to scans with
	// unbounded maximum cardinality. This helps prevent surprising plans for very
	// small tables or for when stats are stale. For full table scans, this
	// penalty is added on top of the fullScanRowCountPenalty.
	unboundedMaxCardinalityScanRowCountPenalty = fullScanRowCountPenalty

	// largeMaxCardinalityScanRowCountPenalty is the maximum penalty to add to
	// scans with a bounded maximum cardinality exceeding the row count estimate.
	// This helps prevent surprising plans for very small tables or for when stats
	// are stale.
	largeMaxCardinalityScanRowCountPenalty = unboundedMaxCardinalityScanRowCountPenalty / 2

	// preferLookupJoinFactor is a scale factor for the cost of a lookup join when
	// we have a hint for preferring a lookup join.
	preferLookupJoinFactor = 1e-6

	// noSpillRowCount represents the maximum number of rows that should have no
	// buffering cost because we expect they will never need to be spilled to
	// disk. Since 64MB is the default work mem limit, 64 rows will not cause a
	// disk spill unless the rows are at least 1 MB on average.
	noSpillRowCount = 64

	// spillRowCount represents the minimum number of rows that we expect will
	// always need to be spilled to disk. Since 64MB is the default work mem
	// limit, 6400000 rows with an average of at least 10 bytes per row will cause
	// a disk spill.
	spillRowCount = 6400000

	// spillCostFactor is the cost of spilling to disk. We use seqIOCostFactor to
	// model the cost of spilling to disk, because although there will be some
	// random I/O required to insert rows into a sorted structure, the inherent
	// batching in the LSM tree should amortize the cost.
	spillCostFactor = seqIOCostFactor
)

// fnCost maps some functions to an execution cost. Currently this list
// contains only st_* functions, including some we don't have implemented
// yet. Although function costs differ based on the overload (due to
// arguments), here we are using the minimum from similar functions based on
// postgres' pg_proc table. The following query can be used to generate this table:
//   SELECT proname, min(procost) FROM pg_proc WHERE proname LIKE 'st\_%' AND procost > 1 GROUP BY proname ORDER BY proname
// TODO(mjibson): Add costs directly to overloads. When that is done, we should
// also add a test that ensures those costs match postgres.
var fnCost = map[string]memo.Cost{
	"st_3dclosestpoint":           1000 * cpuCostFactor,
	"st_3ddfullywithin":           10000 * cpuCostFactor,
	"st_3ddistance":               1000 * cpuCostFactor,
	"st_3ddwithin":                10000 * cpuCostFactor,
	"st_3dintersects":             10000 * cpuCostFactor,
	"st_3dlength":                 100 * cpuCostFactor,
	"st_3dlongestline":            1000 * cpuCostFactor,
	"st_3dmakebox":                100 * cpuCostFactor,
	"st_3dmaxdistance":            1000 * cpuCostFactor,
	"st_3dperimeter":              100 * cpuCostFactor,
	"st_3dshortestline":           1000 * cpuCostFactor,
	"st_addmeasure":               1000 * cpuCostFactor,
	"st_addpoint":                 100 * cpuCostFactor,
	"st_affine":                   100 * cpuCostFactor,
	"st_angle":                    100 * cpuCostFactor,
	"st_area":                     100 * cpuCostFactor,
	"st_area2d":                   100 * cpuCostFactor,
	"st_asbinary":                 100 * cpuCostFactor,
	"st_asencodedpolyline":        100 * cpuCostFactor,
	"st_asewkb":                   100 * cpuCostFactor,
	"st_asewkt":                   100 * cpuCostFactor,
	"st_asgeojson":                100 * cpuCostFactor,
	"st_asgml":                    100 * cpuCostFactor,
	"st_ashexewkb":                100 * cpuCostFactor,
	"st_askml":                    100 * cpuCostFactor,
	"st_aslatlontext":             100 * cpuCostFactor,
	"st_assvg":                    100 * cpuCostFactor,
	"st_astext":                   100 * cpuCostFactor,
	"st_astwkb":                   1000 * cpuCostFactor,
	"st_asx3d":                    100 * cpuCostFactor,
	"st_azimuth":                  100 * cpuCostFactor,
	"st_bdmpolyfromtext":          100 * cpuCostFactor,
	"st_bdpolyfromtext":           100 * cpuCostFactor,
	"st_boundary":                 1000 * cpuCostFactor,
	"st_boundingdiagonal":         100 * cpuCostFactor,
	"st_box2dfromgeohash":         1000 * cpuCostFactor,
	"st_buffer":                   100 * cpuCostFactor,
	"st_buildarea":                10000 * cpuCostFactor,
	"st_centroid":                 100 * cpuCostFactor,
	"st_chaikinsmoothing":         10000 * cpuCostFactor,
	"st_cleangeometry":            10000 * cpuCostFactor,
	"st_clipbybox2d":              10000 * cpuCostFactor,
	"st_closestpoint":             1000 * cpuCostFactor,
	"st_closestpointofapproach":   10000 * cpuCostFactor,
	"st_clusterdbscan":            10000 * cpuCostFactor,
	"st_clusterintersecting":      10000 * cpuCostFactor,
	"st_clusterkmeans":            10000 * cpuCostFactor,
	"st_clusterwithin":            10000 * cpuCostFactor,
	"st_collectionextract":        100 * cpuCostFactor,
	"st_collectionhomogenize":     100 * cpuCostFactor,
	"st_concavehull":              10000 * cpuCostFactor,
	"st_contains":                 10000 * cpuCostFactor,
	"st_containsproperly":         10000 * cpuCostFactor,
	"st_convexhull":               10000 * cpuCostFactor,
	"st_coorddim":                 100 * cpuCostFactor,
	"st_coveredby":                100 * cpuCostFactor,
	"st_covers":                   100 * cpuCostFactor,
	"st_cpawithin":                10000 * cpuCostFactor,
	"st_createtopogeo":            100 * cpuCostFactor,
	"st_crosses":                  10000 * cpuCostFactor,
	"st_curvetoline":              10000 * cpuCostFactor,
	"st_delaunaytriangles":        10000 * cpuCostFactor,
	"st_dfullywithin":             10000 * cpuCostFactor,
	"st_difference":               10000 * cpuCostFactor,
	"st_dimension":                100 * cpuCostFactor,
	"st_disjoint":                 10000 * cpuCostFactor,
	"st_distance":                 100 * cpuCostFactor,
	"st_distancecpa":              10000 * cpuCostFactor,
	"st_distancesphere":           100 * cpuCostFactor,
	"st_distancespheroid":         1000 * cpuCostFactor,
	"st_dump":                     1000 * cpuCostFactor,
	"st_dumppoints":               100 * cpuCostFactor,
	"st_dumprings":                1000 * cpuCostFactor,
	"st_dwithin":                  100 * cpuCostFactor,
	"st_endpoint":                 100 * cpuCostFactor,
	"st_envelope":                 100 * cpuCostFactor,
	"st_equals":                   10000 * cpuCostFactor,
	"st_expand":                   100 * cpuCostFactor,
	"st_exteriorring":             100 * cpuCostFactor,
	"st_filterbym":                1000 * cpuCostFactor,
	"st_findextent":               100 * cpuCostFactor,
	"st_flipcoordinates":          1000 * cpuCostFactor,
	"st_force2d":                  100 * cpuCostFactor,
	"st_force3d":                  100 * cpuCostFactor,
	"st_force3dm":                 100 * cpuCostFactor,
	"st_force3dz":                 100 * cpuCostFactor,
	"st_force4d":                  100 * cpuCostFactor,
	"st_forcecollection":          100 * cpuCostFactor,
	"st_forcecurve":               1000 * cpuCostFactor,
	"st_forcepolygonccw":          100 * cpuCostFactor,
	"st_forcepolygoncw":           1000 * cpuCostFactor,
	"st_forcerhr":                 1000 * cpuCostFactor,
	"st_forcesfs":                 1000 * cpuCostFactor,
	"st_frechetdistance":          10000 * cpuCostFactor,
	"st_generatepoints":           10000 * cpuCostFactor,
	"st_geogfromtext":             100 * cpuCostFactor,
	"st_geogfromwkb":              100 * cpuCostFactor,
	"st_geographyfromtext":        100 * cpuCostFactor,
	"st_geohash":                  1000 * cpuCostFactor,
	"st_geomcollfromtext":         100 * cpuCostFactor,
	"st_geomcollfromwkb":          100 * cpuCostFactor,
	"st_geometricmedian":          10000 * cpuCostFactor,
	"st_geometryfromtext":         1000 * cpuCostFactor,
	"st_geometryn":                100 * cpuCostFactor,
	"st_geometrytype":             100 * cpuCostFactor,
	"st_geomfromewkb":             100 * cpuCostFactor,
	"st_geomfromewkt":             100 * cpuCostFactor,
	"st_geomfromgeohash":          1000 * cpuCostFactor,
	"st_geomfromgeojson":          1000 * cpuCostFactor,
	"st_geomfromgml":              100 * cpuCostFactor,
	"st_geomfromkml":              1000 * cpuCostFactor,
	"st_geomfromtext":             1000 * cpuCostFactor,
	"st_geomfromtwkb":             100 * cpuCostFactor,
	"st_geomfromwkb":              100 * cpuCostFactor,
	"st_gmltosql":                 100 * cpuCostFactor,
	"st_hasarc":                   100 * cpuCostFactor,
	"st_hausdorffdistance":        10000 * cpuCostFactor,
	"st_inittopogeo":              100 * cpuCostFactor,
	"st_interiorringn":            100 * cpuCostFactor,
	"st_interpolatepoint":         1000 * cpuCostFactor,
	"st_intersection":             100 * cpuCostFactor,
	"st_intersects":               100 * cpuCostFactor,
	"st_isclosed":                 100 * cpuCostFactor,
	"st_iscollection":             1000 * cpuCostFactor,
	"st_isempty":                  100 * cpuCostFactor,
	"st_ispolygonccw":             100 * cpuCostFactor,
	"st_ispolygoncw":              100 * cpuCostFactor,
	"st_isring":                   1000 * cpuCostFactor,
	"st_issimple":                 1000 * cpuCostFactor,
	"st_isvalid":                  100 * cpuCostFactor,
	"st_isvaliddetail":            10000 * cpuCostFactor,
	"st_isvalidreason":            100 * cpuCostFactor,
	"st_isvalidtrajectory":        10000 * cpuCostFactor,
	"st_length":                   100 * cpuCostFactor,
	"st_length2d":                 100 * cpuCostFactor,
	"st_length2dspheroid":         1000 * cpuCostFactor,
	"st_lengthspheroid":           1000 * cpuCostFactor,
	"st_linecrossingdirection":    10000 * cpuCostFactor,
	"st_linefromencodedpolyline":  1000 * cpuCostFactor,
	"st_linefrommultipoint":       100 * cpuCostFactor,
	"st_linefromtext":             100 * cpuCostFactor,
	"st_linefromwkb":              100 * cpuCostFactor,
	"st_lineinterpolatepoint":     1000 * cpuCostFactor,
	"st_lineinterpolatepoints":    1000 * cpuCostFactor,
	"st_linelocatepoint":          1000 * cpuCostFactor,
	"st_linemerge":                10000 * cpuCostFactor,
	"st_linestringfromwkb":        100 * cpuCostFactor,
	"st_linesubstring":            1000 * cpuCostFactor,
	"st_linetocurve":              10000 * cpuCostFactor,
	"st_locatealong":              1000 * cpuCostFactor,
	"st_locatebetween":            1000 * cpuCostFactor,
	"st_locatebetweenelevations":  1000 * cpuCostFactor,
	"st_longestline":              100 * cpuCostFactor,
	"st_makeenvelope":             100 * cpuCostFactor,
	"st_makeline":                 100 * cpuCostFactor,
	"st_makepoint":                100 * cpuCostFactor,
	"st_makepointm":               100 * cpuCostFactor,
	"st_makepolygon":              100 * cpuCostFactor,
	"st_makevalid":                10000 * cpuCostFactor,
	"st_maxdistance":              100 * cpuCostFactor,
	"st_memsize":                  100 * cpuCostFactor,
	"st_minimumboundingcircle":    10000 * cpuCostFactor,
	"st_minimumboundingradius":    10000 * cpuCostFactor,
	"st_minimumclearance":         10000 * cpuCostFactor,
	"st_minimumclearanceline":     10000 * cpuCostFactor,
	"st_mlinefromtext":            100 * cpuCostFactor,
	"st_mlinefromwkb":             100 * cpuCostFactor,
	"st_mpointfromtext":           100 * cpuCostFactor,
	"st_mpointfromwkb":            100 * cpuCostFactor,
	"st_mpolyfromtext":            100 * cpuCostFactor,
	"st_mpolyfromwkb":             100 * cpuCostFactor,
	"st_multi":                    100 * cpuCostFactor,
	"st_multilinefromwkb":         100 * cpuCostFactor,
	"st_multilinestringfromtext":  100 * cpuCostFactor,
	"st_multipointfromtext":       100 * cpuCostFactor,
	"st_multipointfromwkb":        100 * cpuCostFactor,
	"st_multipolyfromwkb":         100 * cpuCostFactor,
	"st_multipolygonfromtext":     100 * cpuCostFactor,
	"st_node":                     10000 * cpuCostFactor,
	"st_normalize":                100 * cpuCostFactor,
	"st_npoints":                  100 * cpuCostFactor,
	"st_nrings":                   100 * cpuCostFactor,
	"st_numgeometries":            100 * cpuCostFactor,
	"st_numinteriorring":          100 * cpuCostFactor,
	"st_numinteriorrings":         100 * cpuCostFactor,
	"st_numpatches":               100 * cpuCostFactor,
	"st_numpoints":                100 * cpuCostFactor,
	"st_offsetcurve":              10000 * cpuCostFactor,
	"st_orderingequals":           10000 * cpuCostFactor,
	"st_orientedenvelope":         10000 * cpuCostFactor,
	"st_overlaps":                 10000 * cpuCostFactor,
	"st_patchn":                   100 * cpuCostFactor,
	"st_perimeter":                100 * cpuCostFactor,
	"st_perimeter2d":              100 * cpuCostFactor,
	"st_point":                    100 * cpuCostFactor,
	"st_pointfromgeohash":         1000 * cpuCostFactor,
	"st_pointfromtext":            100 * cpuCostFactor,
	"st_pointfromwkb":             100 * cpuCostFactor,
	"st_pointinsidecircle":        1000 * cpuCostFactor,
	"st_pointn":                   100 * cpuCostFactor,
	"st_pointonsurface":           1000 * cpuCostFactor,
	"st_points":                   1000 * cpuCostFactor,
	"st_polyfromtext":             100 * cpuCostFactor,
	"st_polyfromwkb":              100 * cpuCostFactor,
	"st_polygon":                  100 * cpuCostFactor,
	"st_polygonfromtext":          100 * cpuCostFactor,
	"st_polygonfromwkb":           100 * cpuCostFactor,
	"st_polygonize":               10000 * cpuCostFactor,
	"st_project":                  1000 * cpuCostFactor,
	"st_quantizecoordinates":      1000 * cpuCostFactor,
	"st_relate":                   10000 * cpuCostFactor,
	"st_relatematch":              1000 * cpuCostFactor,
	"st_removepoint":              100 * cpuCostFactor,
	"st_removerepeatedpoints":     1000 * cpuCostFactor,
	"st_reverse":                  1000 * cpuCostFactor,
	"st_rotate":                   100 * cpuCostFactor,
	"st_rotatex":                  100 * cpuCostFactor,
	"st_rotatey":                  100 * cpuCostFactor,
	"st_rotatez":                  100 * cpuCostFactor,
	"st_scale":                    100 * cpuCostFactor,
	"st_segmentize":               1000 * cpuCostFactor,
	"st_seteffectivearea":         1000 * cpuCostFactor,
	"st_setpoint":                 100 * cpuCostFactor,
	"st_setsrid":                  100 * cpuCostFactor,
	"st_sharedpaths":              10000 * cpuCostFactor,
	"st_shortestline":             1000 * cpuCostFactor,
	"st_simplify":                 100 * cpuCostFactor,
	"st_simplifypreservetopology": 10000 * cpuCostFactor,
	"st_simplifyvw":               10000 * cpuCostFactor,
	"st_snap":                     10000 * cpuCostFactor,
	"st_snaptogrid":               100 * cpuCostFactor,
	"st_split":                    10000 * cpuCostFactor,
	"st_srid":                     100 * cpuCostFactor,
	"st_startpoint":               100 * cpuCostFactor,
	"st_subdivide":                10000 * cpuCostFactor,
	"st_summary":                  100 * cpuCostFactor,
	"st_swapordinates":            100 * cpuCostFactor,
	"st_symdifference":            10000 * cpuCostFactor,
	"st_symmetricdifference":      10000 * cpuCostFactor,
	"st_tileenvelope":             100 * cpuCostFactor,
	"st_touches":                  10000 * cpuCostFactor,
	"st_transform":                100 * cpuCostFactor,
	"st_translate":                100 * cpuCostFactor,
	"st_transscale":               100 * cpuCostFactor,
	"st_unaryunion":               10000 * cpuCostFactor,
	"st_union":                    10000 * cpuCostFactor,
	"st_voronoilines":             100 * cpuCostFactor,
	"st_voronoipolygons":          100 * cpuCostFactor,
	"st_within":                   10000 * cpuCostFactor,
	"st_wkbtosql":                 100 * cpuCostFactor,
	"st_wkttosql":                 1000 * cpuCostFactor,
}

// Init initializes a new coster structure with the given memo.
func (c *coster) Init(evalCtx *tree.EvalContext, mem *memo.Memo, perturbation float64) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = coster{
		mem:          mem,
		locality:     evalCtx.Locality,
		perturbation: perturbation,
	}
}

// ComputeCost calculates the estimated cost of the top-level operator in a
// candidate best expression, based on its logical properties and those of its
// children.
//
// Note: each custom function to compute the cost of an operator calculates
// the cost based on Big-O estimated complexity. Most constant factors are
// ignored for now.
func (c *coster) ComputeCost(candidate memo.RelExpr, required *physical.Required) memo.Cost {
	var cost memo.Cost
	switch candidate.Op() {
	case opt.SortOp:
		cost = c.computeSortCost(candidate.(*memo.SortExpr), required)

	case opt.ScanOp:
		cost = c.computeScanCost(candidate.(*memo.ScanExpr), required)

	case opt.SelectOp:
		cost = c.computeSelectCost(candidate.(*memo.SelectExpr), required)

	case opt.ProjectOp:
		cost = c.computeProjectCost(candidate.(*memo.ProjectExpr))

	case opt.InvertedFilterOp:
		cost = c.computeInvertedFilterCost(candidate.(*memo.InvertedFilterExpr))

	case opt.ValuesOp:
		cost = c.computeValuesCost(candidate.(*memo.ValuesExpr))

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		// All join ops use hash join by default.
		cost = c.computeHashJoinCost(candidate)

	case opt.MergeJoinOp:
		cost = c.computeMergeJoinCost(candidate.(*memo.MergeJoinExpr))

	case opt.IndexJoinOp:
		cost = c.computeIndexJoinCost(candidate.(*memo.IndexJoinExpr), required)

	case opt.LookupJoinOp:
		cost = c.computeLookupJoinCost(candidate.(*memo.LookupJoinExpr), required)

	case opt.InvertedJoinOp:
		cost = c.computeInvertedJoinCost(candidate.(*memo.InvertedJoinExpr), required)

	case opt.ZigzagJoinOp:
		cost = c.computeZigzagJoinCost(candidate.(*memo.ZigzagJoinExpr))

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp, opt.LocalityOptimizedSearchOp:
		cost = c.computeSetCost(candidate)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp, opt.EnsureDistinctOnOp,
		opt.UpsertDistinctOnOp, opt.EnsureUpsertDistinctOnOp:
		cost = c.computeGroupingCost(candidate, required)

	case opt.LimitOp:
		cost = c.computeLimitCost(candidate.(*memo.LimitExpr))

	case opt.OffsetOp:
		cost = c.computeOffsetCost(candidate.(*memo.OffsetExpr))

	case opt.OrdinalityOp:
		cost = c.computeOrdinalityCost(candidate.(*memo.OrdinalityExpr))

	case opt.ProjectSetOp:
		cost = c.computeProjectSetCost(candidate.(*memo.ProjectSetExpr))

	case opt.ExplainOp:
		// Technically, the cost of an Explain operation is independent of the cost
		// of the underlying plan. However, we want to explain the plan we would get
		// without EXPLAIN, i.e. the lowest cost plan. So do nothing special to get
		// default behavior.
	}

	// Add a one-time cost for any operator, meant to reflect the cost of setting
	// up execution for the operator. This makes plans with fewer operators
	// preferable, all else being equal.
	cost += cpuCostFactor

	if !cost.Less(memo.MaxCost) {
		// Optsteps uses MaxCost to suppress nodes in the memo. When a node with
		// MaxCost is added to the memo, it can lead to an obscure crash with an
		// unknown node. We'd rather detect this early.
		panic(errors.AssertionFailedf("node %s with MaxCost added to the memo", log.Safe(candidate.Op())))
	}

	if c.perturbation != 0 {
		// Don't perturb the cost if we are forcing an index.
		if cost < hugeCost {
			// Get a random value in the range [-1.0, 1.0)
			multiplier := 2*rand.Float64() - 1

			// If perturbation is p, and the estimated cost of an expression is c,
			// the new cost is in the range [max(0, c - pc), c + pc). For example,
			// if p=1.5, the new cost is in the range [0, c + 1.5 * c).
			cost += cost * memo.Cost(c.perturbation*multiplier)
			// The cost must always be >= 0.
			if cost < 0 {
				cost = 0
			}
		}
	}

	return cost
}

func (c *coster) computeSortCost(sort *memo.SortExpr, required *physical.Required) memo.Cost {
	// We calculate the cost of a (potentially) segmented sort.
	//
	// In a non-segmented sort, we have a single segment to sort according to
	// required.Ordering.Columns.
	//
	// In a segmented sort, rows are split into segments according to
	// InputOrdering.Columns; each segment is sorted according to the remaining
	// columns from required.Ordering.Columns.
	numKeyCols := len(required.Ordering.Columns)
	numPreorderedCols := len(sort.InputOrdering.Columns)

	rel := sort.Relational()
	stats := rel.Stats
	numSegments := c.countSegments(sort)

	// Start with a cost of storing each row; this takes the total number of
	// columns into account so that a sort on fewer columns is preferred (e.g.
	// sort before projecting a new column).
	cost := memo.Cost(cpuCostFactor * float64(rel.OutputCols.Len()) * stats.RowCount)

	if !sort.InputOrdering.Any() {
		// Add the cost for finding the segments: each row is compared to the
		// previous row on the preordered columns. Most of these comparisons will
		// yield equality, so we don't use rowCmpCost(): we expect to have to
		// compare all preordered columns.
		cost += cpuCostFactor * memo.Cost(numPreorderedCols) * memo.Cost(stats.RowCount)
	}

	// Add the cost to sort the segments. On average, each row is involved in
	// O(log(segmentSize)) comparisons.
	numCmpOpsPerRow := float64(1)
	if segmentSize := stats.RowCount / numSegments; segmentSize > 1 {
		numCmpOpsPerRow += math.Log2(segmentSize)

		// Add a cost for buffering rows that takes into account increased memory
		// pressure and the possibility of spilling to disk.
		cost += memo.Cost(numSegments) * c.rowBufferCost(segmentSize)
	}
	cost += c.rowCmpCost(numKeyCols-numPreorderedCols) * memo.Cost(numCmpOpsPerRow*stats.RowCount)
	return cost
}

func (c *coster) computeScanCost(scan *memo.ScanExpr, required *physical.Required) memo.Cost {
	// Scanning an index with a few columns is faster than scanning an index with
	// many columns. Ideally, we would want to use statistics about the size of
	// each column. In lieu of that, use the number of columns.
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index {
		// If we are forcing an index, any other index has a very high cost. In
		// practice, this will only happen when this is a primary index scan.
		return hugeCost
	}
	rowCount := scan.Relational().Stats.RowCount
	perRowCost := c.rowScanCost(scan.Table, scan.Index, scan.Cols.Len())

	numSpans := 1
	if scan.Constraint != nil {
		numSpans = scan.Constraint.Spans.Count()
	} else if scan.InvertedConstraint != nil {
		numSpans = len(scan.InvertedConstraint)
	}
	baseCost := memo.Cost(numSpans * randIOCostFactor)

	// If this is a virtual scan, add the cost of fetching table descriptors.
	if c.mem.Metadata().Table(scan.Table).IsVirtualTable() {
		baseCost += virtualScanTableDescriptorFetchCost
	}

	// Performing a reverse scan is more expensive than a forward scan, but it's
	// still preferable to sorting the output of a forward scan. To ensure we
	// choose a reverse scan over a sort, add the reverse scan cost before we
	// alter the row count for unbounded scan penalties below. This cost must also
	// be added before adjusting the row count for the limit hint.
	if ordering.ScanIsReverse(scan, &required.Ordering) {
		if rowCount > 1 {
			// Need to do binary search to seek to the previous row.
			perRowCost += memo.Cost(math.Log2(rowCount)) * cpuCostFactor
		}
	}

	// Add a penalty to full table scans. All else being equal, we prefer a
	// constrained scan. Adding a few rows worth of cost helps prevent surprising
	// plans for very small tables.
	if scan.IsUnfiltered(c.mem.Metadata()) {
		rowCount += fullScanRowCountPenalty

		// For tables with multiple partitions, add the cost of visiting each
		// partition.
		// TODO(rytaft): In the future we should take latency into account here.
		index := c.mem.Metadata().Table(scan.Table).Index(scan.Index)
		if partitionCount := index.PartitionCount(); partitionCount > 1 {
			// Subtract 1 since we already accounted for the first partition when
			// counting spans.
			baseCost += memo.Cost(partitionCount-1) * randIOCostFactor
		}
	}

	// Add a penalty if the cardinality exceeds the row count estimate. Adding a
	// few rows worth of cost helps prevent surprising plans for very small tables
	// or for when stats are stale.
	rowCount += c.largeCardinalityRowCountPenalty(scan.Relational().Cardinality, rowCount)

	if required.LimitHint != 0 {
		rowCount = math.Min(rowCount, required.LimitHint)
	}

	cost := baseCost + memo.Cost(rowCount)*(seqIOCostFactor+perRowCost)

	// If this scan is locality optimized, divide the cost by 3 in order to make
	// the total cost of the two scans in the locality optimized plan less than
	// the cost of the single scan in the non-locality optimized plan.
	// TODO(rytaft): This is hacky. We should really be making this determination
	// based on the latency between regions.
	if scan.LocalityOptimized {
		cost /= 3
	}
	return cost
}

func (c *coster) computeSelectCost(sel *memo.SelectExpr, required *physical.Required) memo.Cost {
	// Typically the filter has to be evaluated on each input row.
	inputRowCount := sel.Input.Relational().Stats.RowCount

	// If there is a LimitHint, n, it is expected that the filter will only be
	// evaluated on the number of rows required to produce n rows.
	if required.LimitHint != 0 {
		selectivity := sel.Relational().Stats.Selectivity.AsFloat()
		inputRowCount = math.Min(inputRowCount, required.LimitHint/selectivity)
	}

	filterSetup, filterPerRow := c.computeFiltersCost(sel.Filters, util.FastIntMap{})
	cost := memo.Cost(inputRowCount) * filterPerRow
	cost += filterSetup
	return cost
}

func (c *coster) computeProjectCost(prj *memo.ProjectExpr) memo.Cost {
	// Each synthesized column causes an expression to be evaluated on each row.
	rowCount := prj.Relational().Stats.RowCount
	synthesizedColCount := len(prj.Projections)
	cost := memo.Cost(rowCount) * memo.Cost(synthesizedColCount) * cpuCostFactor

	// Add the CPU cost of emitting the rows.
	cost += memo.Cost(rowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeInvertedFilterCost(invFilter *memo.InvertedFilterExpr) memo.Cost {
	// The filter has to be evaluated on each input row.
	inputRowCount := invFilter.Input.Relational().Stats.RowCount
	cost := memo.Cost(inputRowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeValuesCost(values *memo.ValuesExpr) memo.Cost {
	return memo.Cost(values.Relational().Stats.RowCount) * cpuCostFactor
}

func (c *coster) computeHashJoinCost(join memo.RelExpr) memo.Cost {
	if join.Private().(*memo.JoinPrivate).Flags.Has(memo.DisallowHashJoinStoreRight) {
		return hugeCost
	}
	leftRowCount := join.Child(0).(memo.RelExpr).Relational().Stats.RowCount
	rightRowCount := join.Child(1).(memo.RelExpr).Relational().Stats.RowCount
	if (join.Op() == opt.SemiJoinOp || join.Op() == opt.AntiJoinOp) && leftRowCount < rightRowCount {
		// If we have a semi or an anti join, during the execbuilding we choose
		// the relation with smaller cardinality to be on the right side, so we
		// need to swap row counts accordingly.
		// TODO(raduberinde): we might also need to look at memo.JoinFlags when
		// choosing a side.
		leftRowCount, rightRowCount = rightRowCount, leftRowCount
	}

	// A hash join must process every row from both tables once.
	//
	// We add some factors to account for the hashtable build and lookups. The
	// right side is the one stored in the hashtable, so we use a larger factor
	// for that side. This ensures that a join with the smaller right side is
	// preferred to the symmetric join.
	cost := memo.Cost(1.25*leftRowCount+1.75*rightRowCount) * cpuCostFactor

	// Add a cost for buffering rows that takes into account increased memory
	// pressure and the possibility of spilling to disk.
	cost += c.rowBufferCost(rightRowCount)

	// Compute filter cost. Fetch the equality columns so they can be
	// ignored later.
	on := join.Child(2).(*memo.FiltersExpr)
	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		join.Child(0).(memo.RelExpr).Relational().OutputCols,
		join.Child(1).(memo.RelExpr).Relational().OutputCols,
		*on,
	)
	// Generate a quick way to lookup if two columns are join equality
	// columns. We add in both directions because we don't know which way
	// the equality filters will be defined.
	eqMap := util.FastIntMap{}
	for i := range leftEq {
		left := int(leftEq[i])
		right := int(rightEq[i])
		eqMap.Set(left, right)
		eqMap.Set(right, left)
	}
	filterSetup, filterPerRow := c.computeFiltersCost(*on, eqMap)
	cost += filterSetup

	// Add the CPU cost of emitting the rows.
	rowsProcessed, ok := c.mem.RowsProcessed(join)
	if !ok {
		// This can happen as part of testing. In this case just return the number
		// of rows.
		rowsProcessed = join.Relational().Stats.RowCount
	}
	cost += memo.Cost(rowsProcessed) * filterPerRow

	return cost
}

func (c *coster) computeMergeJoinCost(join *memo.MergeJoinExpr) memo.Cost {
	if join.MergeJoinPrivate.Flags.Has(memo.DisallowMergeJoin) {
		return hugeCost
	}
	leftRowCount := join.Left.Relational().Stats.RowCount
	rightRowCount := join.Right.Relational().Stats.RowCount

	cost := memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor

	filterSetup, filterPerRow := c.computeFiltersCost(join.On, util.FastIntMap{})
	cost += filterSetup

	// Add the CPU cost of emitting the rows.
	rowsProcessed, ok := c.mem.RowsProcessed(join)
	if !ok {
		// We shouldn't ever get here. Since we don't allow the memo
		// to be optimized twice, the coster should never be used after
		// logPropsBuilder.clear() is called.
		panic(errors.AssertionFailedf("could not get rows processed for merge join"))
	}
	cost += memo.Cost(rowsProcessed) * filterPerRow
	return cost
}

func (c *coster) computeIndexJoinCost(
	join *memo.IndexJoinExpr, required *physical.Required,
) memo.Cost {
	return c.computeIndexLookupJoinCost(
		join,
		required,
		true, /* lookupColsAreTableKey */
		memo.TrueFilter,
		join.Cols,
		join.Table,
		cat.PrimaryIndex,
		memo.JoinFlags(0),
		false, /* localityOptimized */
	)
}

func (c *coster) computeLookupJoinCost(
	join *memo.LookupJoinExpr, required *physical.Required,
) memo.Cost {
	if join.LookupJoinPrivate.Flags.Has(memo.DisallowLookupJoinIntoRight) {
		return hugeCost
	}
	return c.computeIndexLookupJoinCost(
		join,
		required,
		join.LookupColsAreTableKey,
		join.On,
		join.Cols,
		join.Table,
		join.Index,
		join.Flags,
		join.LocalityOptimized,
	)
}

func (c *coster) computeIndexLookupJoinCost(
	join memo.RelExpr,
	required *physical.Required,
	lookupColsAreTableKey bool,
	on memo.FiltersExpr,
	cols opt.ColSet,
	table opt.TableID,
	index cat.IndexOrdinal,
	flags memo.JoinFlags,
	localityOptimized bool,
) memo.Cost {
	input := join.Child(0).(memo.RelExpr)
	lookupCount := input.Relational().Stats.RowCount

	// Take into account that the "internal" row count is higher, according to
	// the selectivities of the conditions. In particular, we need to ignore
	// left-over conditions that are not selective.
	// For example:
	//   ab JOIN xy ON a=x AND x=10
	// becomes (during normalization):
	//   ab JOIN xy ON a=x AND a=10 AND x=10
	// which can become a lookup join with left-over condition x=10 which doesn't
	// actually filter anything.
	rowsProcessed, ok := c.mem.RowsProcessed(join)
	if !ok {
		// We shouldn't ever get here. Since we don't allow the memo
		// to be optimized twice, the coster should never be used after
		// logPropsBuilder.clear() is called.
		panic(errors.AssertionFailedf("could not get rows processed for lookup join"))
	}

	// Lookup joins can return early if enough rows have been found. An otherwise
	// expensive lookup join might have a lower cost if its limit hint estimates
	// that most rows will not be needed.
	if required.LimitHint != 0 && lookupCount > 0 {
		outputRows := join.Relational().Stats.RowCount
		unlimitedLookupCount := lookupCount
		lookupCount = lookupJoinInputLimitHint(unlimitedLookupCount, outputRows, required.LimitHint)
		// We scale the number of rows processed by the same factor (we are
		// calculating the average number of rows processed per lookup and
		// multiplying by the new lookup count).
		rowsProcessed = (rowsProcessed / unlimitedLookupCount) * lookupCount
	}

	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range, this
	// counts as random I/O.
	perLookupCost := memo.Cost(randIOCostFactor)
	if !lookupColsAreTableKey {
		// If the lookup columns don't form a key, execution will have to limit
		// KV batches which prevents running requests to multiple nodes in parallel.
		// An experiment on a 4 node cluster with a table with 100k rows split into
		// 100 ranges showed that a "non-parallel" lookup join is about 5 times
		// slower.
		perLookupCost *= 5
	}
	if c.mem.Metadata().Table(table).IsVirtualTable() {
		// It's expensive to perform a lookup join into a virtual table because
		// we need to fetch the table descriptors on each lookup.
		perLookupCost += virtualScanTableDescriptorFetchCost
	}
	perLookupCost += lookupExprCost(join)
	cost := memo.Cost(lookupCount) * perLookupCost

	filterSetup, filterPerRow := c.computeFiltersCost(on, util.FastIntMap{})
	cost += filterSetup

	// Each lookup might retrieve many rows; add the IO cost of retrieving the
	// rows (relevant when we expect many resulting rows per lookup) and the CPU
	// cost of emitting the rows.
	numLookupCols := cols.Difference(input.Relational().OutputCols).Len()
	perRowCost := lookupJoinRetrieveRowCost + filterPerRow +
		c.rowScanCost(table, index, numLookupCols)

	cost += memo.Cost(rowsProcessed) * perRowCost

	if flags.Has(memo.PreferLookupJoinIntoRight) {
		// If we prefer a lookup join, make the cost much smaller.
		cost *= preferLookupJoinFactor
	}

	// If this lookup join is locality optimized, divide the cost by 2.5 in order to make
	// the total cost of the two lookup joins in the locality optimized plan less than
	// the cost of the single lookup join in the non-locality optimized plan.
	// TODO(rytaft): This is hacky. We should really be making this determination
	// based on the latency between regions.
	if localityOptimized {
		cost /= 2.5
	}
	return cost
}

func (c *coster) computeInvertedJoinCost(
	join *memo.InvertedJoinExpr, required *physical.Required,
) memo.Cost {
	if join.InvertedJoinPrivate.Flags.Has(memo.DisallowInvertedJoinIntoRight) {
		return hugeCost
	}
	lookupCount := join.Input.Relational().Stats.RowCount

	// Take into account that the "internal" row count is higher, according to
	// the selectivities of the conditions. In particular, we need to ignore
	// the conditions that don't affect the number of rows processed.
	// A contrived example, where gid is a SERIAL PK:
	//   nyc_census_blocks c JOIN nyc_neighborhoods n ON
	//   ST_Intersects(c.geom, n.geom) AND c.gid < n.gid
	// which can become a lookup join with left-over condition c.gid <
	// n.gid.
	rowsProcessed, ok := c.mem.RowsProcessed(join)
	if !ok {
		// We shouldn't ever get here. Since we don't allow the memo
		// to be optimized twice, the coster should never be used after
		// logPropsBuilder.clear() is called.
		panic(errors.AssertionFailedf("could not get rows processed for inverted join"))
	}

	// Lookup joins can return early if enough rows have been found. An otherwise
	// expensive lookup join might have a lower cost if its limit hint estimates
	// that most rows will not be needed.
	if required.LimitHint != 0 && lookupCount > 0 {
		outputRows := join.Relational().Stats.RowCount
		unlimitedLookupCount := lookupCount
		lookupCount = lookupJoinInputLimitHint(unlimitedLookupCount, outputRows, required.LimitHint)
		// We scale the number of rows processed by the same factor (we are
		// calculating the average number of rows processed per lookup and
		// multiplying by the new lookup count).
		rowsProcessed = (rowsProcessed / unlimitedLookupCount) * lookupCount
	}

	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range, this
	// counts as random I/O.
	perLookupCost := memo.Cost(randIOCostFactor)
	// Since inverted indexes can't form a key, execution will have to
	// limit KV batches which prevents running requests to multiple nodes
	// in parallel.  An experiment on a 4 node cluster with a table with
	// 100k rows split into 100 ranges showed that a "non-parallel" lookup
	// join is about 5 times slower.
	perLookupCost *= 5
	cost := memo.Cost(lookupCount) * perLookupCost

	filterSetup, filterPerRow := c.computeFiltersCost(join.On, util.FastIntMap{})
	cost += filterSetup

	// Each lookup might retrieve many rows; add the IO cost of retrieving the
	// rows (relevant when we expect many resulting rows per lookup) and the CPU
	// cost of emitting the rows.
	numLookupCols := join.Cols.Difference(join.Input.Relational().OutputCols).Len()
	perRowCost := lookupJoinRetrieveRowCost + filterPerRow +
		c.rowScanCost(join.Table, join.Index, numLookupCols)

	cost += memo.Cost(rowsProcessed) * perRowCost
	return cost
}

// computeFiltersCost returns the setup and per-row cost of executing
// a filter. Callers of this function should add setupCost and multiply
// perRowCost by the number of rows expected to be filtered.
func (c *coster) computeFiltersCost(
	filters memo.FiltersExpr, eqMap util.FastIntMap,
) (setupCost, perRowCost memo.Cost) {
	// Add a base perRowCost so that callers do not need to have their own
	// base per-row cost.
	perRowCost += cpuCostFactor
	for i := range filters {
		f := &filters[i]
		switch f.Condition.Op() {
		case opt.EqOp:
			eq := f.Condition.(*memo.EqExpr)
			leftVar, ok := eq.Left.(*memo.VariableExpr)
			if !ok {
				break
			}
			rightVar, ok := eq.Right.(*memo.VariableExpr)
			if !ok {
				break
			}
			if val, ok := eqMap.Get(int(leftVar.Col)); ok && val == int(rightVar.Col) {
				// Equality filters on some joins are still in
				// filters, while others have already removed
				// them. They do not cost anything.
				continue
			}
		case opt.FunctionOp:
			function := f.Condition.(*memo.FunctionExpr)
			// We are ok with the zero value here for functions not in the map.
			perRowCost += fnCost[function.Name]
		}

		// Add a constant "setup" cost per ON condition to account for the fact that
		// the rowsProcessed estimate alone cannot effectively discriminate between
		// plans when RowCount is too small.
		setupCost += cpuCostFactor
	}
	return setupCost, perRowCost
}

func (c *coster) computeZigzagJoinCost(join *memo.ZigzagJoinExpr) memo.Cost {
	rowCount := join.Relational().Stats.RowCount

	// Assume the upper bound on scan cost to be the sum of the cost of
	// scanning the two constituent indexes. To determine how many columns
	// are returned from each scan, intersect the output column set join.Cols
	// with each side's IndexColumns. Columns present in both indexes are
	// projected from the left side only.
	md := c.mem.Metadata()
	leftCols := md.TableMeta(join.LeftTable).IndexColumns(join.LeftIndex)
	leftCols.IntersectionWith(join.Cols)
	rightCols := md.TableMeta(join.RightTable).IndexColumns(join.RightIndex)
	rightCols.IntersectionWith(join.Cols)
	rightCols.DifferenceWith(leftCols)
	scanCost := c.rowScanCost(join.LeftTable, join.LeftIndex, leftCols.Len())
	scanCost += c.rowScanCost(join.RightTable, join.RightIndex, rightCols.Len())

	filterSetup, filterPerRow := c.computeFiltersCost(join.On, util.FastIntMap{})

	// Add a penalty if the cardinality exceeds the row count estimate. Adding a
	// few rows worth of cost helps prevent surprising plans for very small tables
	// or for when stats are stale. This is also needed to ensure parity with the
	// cost of scans.
	rowCount += c.largeCardinalityRowCountPenalty(join.Relational().Cardinality, rowCount)

	// Double the cost of emitting rows as well as the cost of seeking rows,
	// given two indexes will be accessed.
	cost := memo.Cost(rowCount) * (2*(cpuCostFactor+seqIOCostFactor) + scanCost + filterPerRow)
	cost += filterSetup
	return cost
}

func (c *coster) computeSetCost(set memo.RelExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	outputRowCount := set.Relational().Stats.RowCount
	cost := memo.Cost(outputRowCount) * cpuCostFactor

	// A set operation must process every row from both tables once. UnionAll and
	// LocalityOptimizedSearch can avoid any extra computation, but all other set
	// operations must perform a hash table lookup or update for each input row.
	//
	// The exception is if this is a streaming set operation, in which case there
	// is no need to build a hash table. We can detect that this is a streaming
	// operation by checking whether the ordering is defined in the set private.
	if set.Op() != opt.UnionAllOp && set.Op() != opt.LocalityOptimizedSearchOp &&
		set.Private().(*memo.SetPrivate).Ordering.Any() {
		leftRowCount := set.Child(0).(memo.RelExpr).Relational().Stats.RowCount
		rightRowCount := set.Child(1).(memo.RelExpr).Relational().Stats.RowCount
		cost += memo.Cost(leftRowCount+rightRowCount) * cpuCostFactor

		// Add a cost for buffering rows that takes into account increased memory
		// pressure and the possibility of spilling to disk.
		switch set.Op() {
		case opt.UnionOp:
			// Hash Union is implemented as UnionAll followed by Hash Distinct.
			cost += c.rowBufferCost(outputRowCount)

		case opt.IntersectOp, opt.ExceptOp:
			// Hash Intersect and Except are implemented as Hash Distinct on each
			// input followed by a Hash Join that builds the hash table from the right
			// input.
			cost += c.rowBufferCost(leftRowCount) + 2*c.rowBufferCost(rightRowCount)

		case opt.IntersectAllOp, opt.ExceptAllOp:
			// Hash IntersectAll and ExceptAll are implemented as a Hash Join that
			// builds the hash table from the right input.
			cost += c.rowBufferCost(rightRowCount)

		default:
			panic(errors.AssertionFailedf("unhandled operator %s", set.Op()))
		}
	}

	return cost
}

func (c *coster) computeGroupingCost(grouping memo.RelExpr, required *physical.Required) memo.Cost {
	// Start with some extra fixed overhead, since the grouping operators have
	// setup overhead that is greater than other operators like Project. This
	// can matter for rules like ReplaceMaxWithLimit.
	cost := memo.Cost(cpuCostFactor)

	// Add the CPU cost of emitting the rows.
	outputRowCount := grouping.Relational().Stats.RowCount
	cost += memo.Cost(outputRowCount) * cpuCostFactor

	// GroupBy must process each input row once. Cost per row depends on the
	// number of grouping columns and the number of aggregates.
	inputRowCount := grouping.Child(0).(memo.RelExpr).Relational().Stats.RowCount
	aggsCount := grouping.Child(1).ChildCount()
	private := grouping.Private().(*memo.GroupingPrivate)
	groupingColCount := private.GroupingCols.Len()
	cost += memo.Cost(inputRowCount) * memo.Cost(aggsCount+groupingColCount) * cpuCostFactor

	if groupingColCount > 0 {
		// Add a cost that reflects the use of a hash table - unless we are doing a
		// streaming aggregation where all the grouping columns are ordered.
		//
		// The cost is chosen so that it's always less than the cost to sort the
		// input.
		n := len(ordering.StreamingGroupingColOrdering(private, &required.Ordering))
		if groupingColCount > n {
			// Add the cost to build the hash table.
			cost += memo.Cost(inputRowCount) * cpuCostFactor

			// Add a cost for buffering rows that takes into account increased memory
			// pressure and the possibility of spilling to disk.
			cost += c.rowBufferCost(outputRowCount)
		}
	}

	return cost
}

func (c *coster) computeLimitCost(limit *memo.LimitExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(limit.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeOffsetCost(offset *memo.OffsetExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(offset.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeOrdinalityCost(ord *memo.OrdinalityExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(ord.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

func (c *coster) computeProjectSetCost(projectSet *memo.ProjectSetExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost(projectSet.Relational().Stats.RowCount) * cpuCostFactor
	return cost
}

// countSegments calculates the number of segments that will be used to execute
// the sort. If no input ordering is provided, there's only one segment.
func (c *coster) countSegments(sort *memo.SortExpr) float64 {
	if sort.InputOrdering.Any() {
		return 1
	}
	stats := sort.Relational().Stats
	orderedCols := sort.InputOrdering.ColSet()
	orderedStats, ok := stats.ColStats.Lookup(orderedCols)
	if !ok {
		orderedStats, ok = c.mem.RequestColStat(sort, orderedCols)
		if !ok {
			// I don't think we can ever get here. Since we don't allow the memo
			// to be optimized twice, the coster should never be used after
			// logPropsBuilder.clear() is called.
			panic(errors.AssertionFailedf("could not request the stats for ColSet %v", orderedCols))
		}
	}

	return orderedStats.DistinctCount
}

// rowCmpCost is the CPU cost to compare a pair of rows, which depends on the
// number of columns in the sort key.
func (c *coster) rowCmpCost(numKeyCols int) memo.Cost {
	// Sorting involves comparisons on the key columns, but the cost isn't
	// directly proportional: we only compare the second column if the rows are
	// equal on the first column; and so on. We also account for a fixed
	// "non-comparison" cost related to processing the
	// row. The formula is:
	//
	//   cpuCostFactor * [ 1 + Sum eqProb^(i-1) with i=1 to numKeyCols ]
	//
	const eqProb = 0.1
	cost := cpuCostFactor
	for i, c := 0, cpuCostFactor; i < numKeyCols; i, c = i+1, c*eqProb {
		// c is cpuCostFactor * eqProb^i.
		cost += c
	}

	// There is a fixed "non-comparison" cost and a comparison cost proportional
	// to the key columns. Note that the cost has to be high enough so that a
	// sort is almost always more expensive than a reverse scan or an index scan.
	return memo.Cost(cost)
}

// rowScanCost is the CPU cost to scan one row, which depends on the number of
// columns in the index and (to a lesser extent) on the number of columns we are
// scanning.
func (c *coster) rowScanCost(tabID opt.TableID, idxOrd int, numScannedCols int) memo.Cost {
	md := c.mem.Metadata()
	tab := md.Table(tabID)
	idx := tab.Index(idxOrd)
	numCols := idx.ColumnCount()
	// Remove any system columns from numCols.
	for i := 0; i < idx.ColumnCount(); i++ {
		if idx.Column(i).Kind() == cat.System {
			numCols--
		}
	}

	// Adjust cost based on how well the current locality matches the index's
	// zone constraints.
	var costFactor memo.Cost = cpuCostFactor
	if !tab.IsVirtualTable() && len(c.locality.Tiers) != 0 {
		// If 0% of locality tiers have matching constraints, then add additional
		// cost. If 100% of locality tiers have matching constraints, then add no
		// additional cost. Anything in between is proportional to the number of
		// matches.
		adjustment := 1.0 - localityMatchScore(idx.Zone(), c.locality)
		costFactor += latencyCostFactor * memo.Cost(adjustment)
	}

	// The number of the columns in the index matter because more columns means
	// more data to scan. The number of columns we actually return also matters
	// because that is the amount of data that we could potentially transfer over
	// the network.
	return memo.Cost(numCols+numScannedCols) * costFactor
}

// rowBufferCost adds a cost for buffering rows according to a ramp function:
//
//                  cost
//                 factor
//
//                    |               spillRowCount
//   spillCostFactor _|                  ___________ _ _ _
//                    |                 /
//                    |                /
//                    |               /
//                0  _| _ _ _________/______________________    row
//                    |                                        count
//                         noSpillRowCount
//
// This function models the fact that operators that buffer rows become more
// expensive the more rows they need to buffer, since eventually they will need
// to spill to disk. The exact number of rows that cause spilling to disk varies
// depending on a number of factors that we don't model here. Therefore, we use
// a ramp function rather than a step function to account for the uncertainty
// and avoid sudden surprising plan changes due to a small change in stats.
func (c *coster) rowBufferCost(rowCount float64) memo.Cost {
	if rowCount <= noSpillRowCount {
		return 0
	}
	var fraction memo.Cost
	if rowCount >= spillRowCount {
		fraction = 1
	} else {
		fraction = memo.Cost(rowCount-noSpillRowCount) / (spillRowCount - noSpillRowCount)
	}

	return memo.Cost(rowCount) * spillCostFactor * fraction
}

// largeCardinalityRowCountPenalty returns a penalty that should be added to the
// row count of scans. It is non-zero for expressions with unbounded maximum
// cardinality or with maximum cardinality exceeding the row count estimate.
// Adding a few rows worth of cost helps prevent surprising plans for very small
// tables or for when stats are stale.
func (c *coster) largeCardinalityRowCountPenalty(
	cardinality props.Cardinality, rowCount float64,
) float64 {
	if cardinality.IsUnbounded() {
		return unboundedMaxCardinalityScanRowCountPenalty
	}
	if maxCard := float64(cardinality.Max); maxCard > rowCount {
		penalty := maxCard - rowCount
		if penalty > largeMaxCardinalityScanRowCountPenalty {
			penalty = largeMaxCardinalityScanRowCountPenalty
		}
		return penalty
	}
	return 0
}

// localityMatchScore returns a number from 0.0 to 1.0 that describes how well
// the current node's locality matches the given zone constraints and
// leaseholder preferences, with 0.0 indicating 0% and 1.0 indicating 100%. This
// is the basic algorithm:
//
//   t = total # of locality tiers
//
//   Match each locality tier against the constraint set, and compute a value
//   for each tier:
//
//      0 = key not present in constraint set or key matches prohibited
//          constraint, but value doesn't match
//     +1 = key matches required constraint, and value does match
//     -1 = otherwise
//
//   m = length of longest locality prefix that ends in a +1 value and doesn't
//       contain a -1 value.
//
//   Compute "m" for both the ReplicaConstraints constraints set, as well as for
//   the LeasePreferences constraints set:
//
//     constraint-score = m / t
//     lease-pref-score = m / t
//
//   if there are no lease preferences, then final-score = lease-pref-score
//   else final-score = (constraint-score * 2 + lease-pref-score) / 3
//
// Here are some scoring examples:
//
//   Locality = region=us,dc=east
//   0.0 = []                     // No constraints to match
//   0.0 = [+region=eu,+dc=uk]    // None of the tiers match
//   0.0 = [+region=eu,+dc=east]  // 2nd tier matches, but 1st tier doesn't
//   0.0 = [-region=us,+dc=east]  // 1st tier matches PROHIBITED constraint
//   0.0 = [-region=eu]           // 1st tier PROHIBITED and non-matching
//   0.5 = [+region=us]           // 1st tier matches
//   0.5 = [+region=us,-dc=east]  // 1st tier matches, 2nd tier PROHIBITED
//   0.5 = [+region=us,+dc=west]  // 1st tier matches, but 2nd tier doesn't
//   1.0 = [+region=us,+dc=east]  // Both tiers match
//   1.0 = [+dc=east]             // 2nd tier matches, no constraints for 1st
//   1.0 = [+region=us,+dc=east,+rack=1,-ssd]  // Extra constraints ignored
//
// Note that constraints need not be specified in any particular order, so all
// constraints are scanned when matching each locality tier. In cases where
// there are multiple replica constraint groups (i.e. where a subset of replicas
// can have different constraints than another subset), the minimum constraint
// score among the groups is used.
//
// While matching leaseholder preferences are considered in the final score,
// leaseholder preferences are not guaranteed, so its score is weighted at half
// of the replica constraint score, in order to reflect the possibility that the
// leaseholder has moved from the preferred location.
func localityMatchScore(zone cat.Zone, locality roachpb.Locality) float64 {
	// Fast path: if there are no constraints or leaseholder preferences, then
	// locality can't match.
	if zone.ReplicaConstraintsCount() == 0 && zone.LeasePreferenceCount() == 0 {
		return 0.0
	}

	// matchTier matches a tier to a set of constraints and returns:
	//
	//    0 = key not present in constraint set or key only matches prohibited
	//        constraints where value doesn't match
	//   +1 = key matches any required constraint key + value
	//   -1 = otherwise
	//
	matchTier := func(tier roachpb.Tier, set cat.ConstraintSet) int {
		foundNoMatch := false
		for j, n := 0, set.ConstraintCount(); j < n; j++ {
			con := set.Constraint(j)
			if con.GetKey() != tier.Key {
				// Ignore constraints that don't have matching key.
				continue
			}

			if con.GetValue() == tier.Value {
				if !con.IsRequired() {
					// Matching prohibited constraint, so result is -1.
					return -1
				}

				// Matching required constraint, so result is +1.
				return +1
			}

			if con.IsRequired() {
				// Remember that non-matching required constraint was found.
				foundNoMatch = true
			}
		}

		if foundNoMatch {
			// At least one non-matching required constraint was found, and no
			// matching constraints.
			return -1
		}

		// Key not present in constraint set, or key only matches prohibited
		// constraints where value doesn't match.
		return 0
	}

	// matchConstraints returns the number of tiers that match the given
	// constraint set ("m" in algorithm described above).
	matchConstraints := func(set cat.ConstraintSet) int {
		matchCount := 0
		for i, tier := range locality.Tiers {
			switch matchTier(tier, set) {
			case +1:
				matchCount = i + 1
			case -1:
				return matchCount
			}
		}
		return matchCount
	}

	// Score any replica constraints.
	var constraintScore float64
	if zone.ReplicaConstraintsCount() != 0 {
		// Iterate over the replica constraints and determine the minimum value
		// returned by matchConstraints for any replica. For example:
		//
		//   3: [+region=us,+dc=east]
		//   2: [+region=us]
		//
		// For the [region=us,dc=east] locality, the result is min(2, 1).
		minCount := intsets.MaxInt
		for i := 0; i < zone.ReplicaConstraintsCount(); i++ {
			matchCount := matchConstraints(zone.ReplicaConstraints(i))
			if matchCount < minCount {
				minCount = matchCount
			}
		}

		constraintScore = float64(minCount) / float64(len(locality.Tiers))
	}

	// If there are no lease preferences, then use replica constraint score.
	if zone.LeasePreferenceCount() == 0 {
		return constraintScore
	}

	// Score the first lease preference, if one is available. Ignore subsequent
	// lease preferences, since they only apply in edge cases.
	matchCount := matchConstraints(zone.LeasePreference(0))
	leaseScore := float64(matchCount) / float64(len(locality.Tiers))

	// Weight the constraintScore twice as much as the lease score.
	return (constraintScore*2 + leaseScore) / 3
}

// lookupJoinInputLimitHint calculates an appropriate limit hint for the input
// to a lookup join.
func lookupJoinInputLimitHint(inputRowCount, outputRowCount, outputLimitHint float64) float64 {
	if outputRowCount == 0 {
		return 0
	}

	// Estimate the number of lookups needed to output LimitHint rows.
	expectedLookupCount := outputLimitHint * inputRowCount / outputRowCount

	// Round up to the nearest multiple of a batch.
	expectedLookupCount = math.Ceil(expectedLookupCount/joinReaderBatchSize) * joinReaderBatchSize
	return math.Min(inputRowCount, expectedLookupCount)
}

// lookupExprCost accounts for the extra CPU cost of the lookupExpr.
func lookupExprCost(join memo.RelExpr) memo.Cost {
	lookupExpr, ok := join.(*memo.LookupJoinExpr)
	if ok {
		return cpuCostFactor * memo.Cost(len(lookupExpr.LookupExpr))
	}
	return 0
}
