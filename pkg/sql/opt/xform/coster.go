// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"context"
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/distribution"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
//  1. Max aggregate cluster throughput (txns/sec across cluster)
//  2. Min transaction latency (time to commit txns)
//  3. Min latency to first row (time to get first row of txns)
//  4. Min memory usage
//  5. Some weighted combination of #1 - #4
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

	// MaybeGetBestCostRelation returns the best-cost relation for the given memo
	// group if the group has been fully optimized for the `required` physical
	// properties.
	MaybeGetBestCostRelation(grp memo.RelExpr, required *physical.Required) (best memo.RelExpr, ok bool)
}

// coster encapsulates the default cost model for the optimizer. The coster
// assigns an estimated cost to each expression in the memo so that the
// optimizer can choose the lowest cost expression tree. The estimated cost is
// a best-effort approximation of the actual cost of execution, based on table
// and index statistics that are propagated throughout the logical expression
// tree.
type coster struct {
	ctx     context.Context
	evalCtx *eval.Context
	mem     *memo.Memo

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

	// rng is used for deterministic perturbation.
	rng *rand.Rand

	o *Optimizer
}

var _ Coster = &coster{}

// MakeDefaultCoster creates an instance of the default coster.
func MakeDefaultCoster(
	ctx context.Context, evalCtx *eval.Context, mem *memo.Memo, o *Optimizer,
) Coster {
	return &coster{
		ctx:      ctx,
		evalCtx:  evalCtx,
		mem:      mem,
		locality: evalCtx.Locality,
		o:        o,
	}
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

	// fullScanRowCountPenalty adds a penalty to full table scans. This is especially
	// useful for empty or very small tables, where we would get plans that are
	// surprising to users (like full scans instead of point lookups).
	fullScanRowCountPenalty = 10

	// unboundedMaxCardinalityScanCostPenalty adds a penalty to scans with
	// unbounded maximum cardinality. This helps prevent surprising plans for very
	// small tables or for when stats are stale. For full table scans, this
	// penalty is added on top of the fullScanRowCountPenalty.
	unboundedMaxCardinalityScanCostPenalty = 10

	// largeMaxCardinalityScanCostPenalty is the maximum penalty to add to scans
	// with a bounded maximum cardinality exceeding the row count estimate. This
	// helps prevent surprising plans for very small tables or for when stats are
	// stale.
	largeMaxCardinalityScanCostPenalty = unboundedMaxCardinalityScanCostPenalty / 2

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

var (
	// hugeCost is used with expressions we want to avoid; these are expressions
	// that "violate" a hint like forcing a specific index or join algorithm.
	// If the final expression has this cost or larger, it means that there was no
	// plan that could satisfy the hints.
	hugeCost = memo.Cost{C: 1e100, Flags: memo.CostFlags{HugeCostPenalty: true}}

	// SmallDistributeCost is the per-operation cost overhead for scans which may
	// access remote regions, but the scanned table is unpartitioned with no lease
	// preferences, so locality information is not available. The distribution
	// cost is unknown, so a small overhead cost is added to give optimizations
	// like locality-optimized search a chance to get picked.
	SmallDistributeCost = memo.Cost{C: randIOCostFactor}

	// DistributeCost is the per-operation cost overhead for Distribute operations
	// or scans which access remote regions.
	// TODO(msirek): Measure actual latencies between regions and produce a table
	//               for determining the maximum latency between the most remote
	//               region in a distribution and the gateway region.
	DistributeCost = memo.Cost{C: 200}

	// LargeDistributeCost is the cost to use for Distribute operations when a
	// session mode is set to error out on access of rows from remote regions.
	LargeDistributeCost = hugeCost

	// LargeDistributeCostWithHomeRegion is the cost to use for Distribute
	// operations when a session mode is set to error out on access of rows from
	// remote regions, and the query plan has a home region.
	// TODO(msirek): Is there a better way of preferring plans that have a home
	//               region instead of relying on costing, which may not guarantee
	//               the correct plan is found?
	LargeDistributeCostWithHomeRegion = memo.Cost{
		C:     LargeDistributeCost.C / 2,
		Flags: memo.CostFlags{HugeCostPenalty: true},
	}
)

// fnCost maps some functions to an execution cost. Currently this list
// contains only st_* functions, including some we don't have implemented
// yet. Although function costs differ based on the overload (due to
// arguments), here we are using the minimum from similar functions based on
// postgres' pg_proc table. The following query can be used to generate this table:
//
//	SELECT proname, min(procost) FROM pg_proc WHERE proname LIKE 'st\_%' AND procost > 1 GROUP BY proname ORDER BY proname
//
// TODO(mjibson): Add costs directly to overloads. When that is done, we should
// also add a test that ensures those costs match postgres.
var fnCost = map[string]memo.Cost{
	"st_3dclosestpoint":           {C: 1000 * cpuCostFactor},
	"st_3ddfullywithin":           {C: 10000 * cpuCostFactor},
	"st_3ddistance":               {C: 1000 * cpuCostFactor},
	"st_3ddwithin":                {C: 10000 * cpuCostFactor},
	"st_3dintersects":             {C: 10000 * cpuCostFactor},
	"st_3dlength":                 {C: 100 * cpuCostFactor},
	"st_3dlongestline":            {C: 1000 * cpuCostFactor},
	"st_3dmakebox":                {C: 100 * cpuCostFactor},
	"st_3dmaxdistance":            {C: 1000 * cpuCostFactor},
	"st_3dperimeter":              {C: 100 * cpuCostFactor},
	"st_3dshortestline":           {C: 1000 * cpuCostFactor},
	"st_addmeasure":               {C: 1000 * cpuCostFactor},
	"st_addpoint":                 {C: 100 * cpuCostFactor},
	"st_affine":                   {C: 100 * cpuCostFactor},
	"st_angle":                    {C: 100 * cpuCostFactor},
	"st_area":                     {C: 100 * cpuCostFactor},
	"st_area2d":                   {C: 100 * cpuCostFactor},
	"st_asbinary":                 {C: 100 * cpuCostFactor},
	"st_asencodedpolyline":        {C: 100 * cpuCostFactor},
	"st_asewkb":                   {C: 100 * cpuCostFactor},
	"st_asewkt":                   {C: 100 * cpuCostFactor},
	"st_asgeojson":                {C: 100 * cpuCostFactor},
	"st_asgml":                    {C: 100 * cpuCostFactor},
	"st_ashexewkb":                {C: 100 * cpuCostFactor},
	"st_askml":                    {C: 100 * cpuCostFactor},
	"st_aslatlontext":             {C: 100 * cpuCostFactor},
	"st_assvg":                    {C: 100 * cpuCostFactor},
	"st_asmvtgeom":                {C: 100 * cpuCostFactor},
	"st_astext":                   {C: 100 * cpuCostFactor},
	"st_astwkb":                   {C: 1000 * cpuCostFactor},
	"st_asx3d":                    {C: 100 * cpuCostFactor},
	"st_azimuth":                  {C: 100 * cpuCostFactor},
	"st_bdmpolyfromtext":          {C: 100 * cpuCostFactor},
	"st_bdpolyfromtext":           {C: 100 * cpuCostFactor},
	"st_boundary":                 {C: 1000 * cpuCostFactor},
	"st_boundingdiagonal":         {C: 100 * cpuCostFactor},
	"st_box2dfromgeohash":         {C: 1000 * cpuCostFactor},
	"st_buffer":                   {C: 100 * cpuCostFactor},
	"st_buildarea":                {C: 10000 * cpuCostFactor},
	"st_centroid":                 {C: 100 * cpuCostFactor},
	"st_chaikinsmoothing":         {C: 10000 * cpuCostFactor},
	"st_cleangeometry":            {C: 10000 * cpuCostFactor},
	"st_clipbybox2d":              {C: 10000 * cpuCostFactor},
	"st_closestpoint":             {C: 1000 * cpuCostFactor},
	"st_closestpointofapproach":   {C: 10000 * cpuCostFactor},
	"st_clusterdbscan":            {C: 10000 * cpuCostFactor},
	"st_clusterintersecting":      {C: 10000 * cpuCostFactor},
	"st_clusterkmeans":            {C: 10000 * cpuCostFactor},
	"st_clusterwithin":            {C: 10000 * cpuCostFactor},
	"st_collectionextract":        {C: 100 * cpuCostFactor},
	"st_collectionhomogenize":     {C: 100 * cpuCostFactor},
	"st_concavehull":              {C: 10000 * cpuCostFactor},
	"st_contains":                 {C: 10000 * cpuCostFactor},
	"st_containsproperly":         {C: 10000 * cpuCostFactor},
	"st_convexhull":               {C: 10000 * cpuCostFactor},
	"st_coorddim":                 {C: 100 * cpuCostFactor},
	"st_coveredby":                {C: 100 * cpuCostFactor},
	"st_covers":                   {C: 100 * cpuCostFactor},
	"st_cpawithin":                {C: 10000 * cpuCostFactor},
	"st_createtopogeo":            {C: 100 * cpuCostFactor},
	"st_crosses":                  {C: 10000 * cpuCostFactor},
	"st_curvetoline":              {C: 10000 * cpuCostFactor},
	"st_delaunaytriangles":        {C: 10000 * cpuCostFactor},
	"st_dfullywithin":             {C: 10000 * cpuCostFactor},
	"st_difference":               {C: 10000 * cpuCostFactor},
	"st_dimension":                {C: 100 * cpuCostFactor},
	"st_disjoint":                 {C: 10000 * cpuCostFactor},
	"st_distance":                 {C: 100 * cpuCostFactor},
	"st_distancecpa":              {C: 10000 * cpuCostFactor},
	"st_distancesphere":           {C: 100 * cpuCostFactor},
	"st_distancespheroid":         {C: 1000 * cpuCostFactor},
	"st_dump":                     {C: 1000 * cpuCostFactor},
	"st_dumppoints":               {C: 100 * cpuCostFactor},
	"st_dumprings":                {C: 1000 * cpuCostFactor},
	"st_dwithin":                  {C: 100 * cpuCostFactor},
	"st_endpoint":                 {C: 100 * cpuCostFactor},
	"st_envelope":                 {C: 100 * cpuCostFactor},
	"st_equals":                   {C: 10000 * cpuCostFactor},
	"st_expand":                   {C: 100 * cpuCostFactor},
	"st_exteriorring":             {C: 100 * cpuCostFactor},
	"st_filterbym":                {C: 1000 * cpuCostFactor},
	"st_findextent":               {C: 100 * cpuCostFactor},
	"st_flipcoordinates":          {C: 1000 * cpuCostFactor},
	"st_force2d":                  {C: 100 * cpuCostFactor},
	"st_force3d":                  {C: 100 * cpuCostFactor},
	"st_force3dm":                 {C: 100 * cpuCostFactor},
	"st_force3dz":                 {C: 100 * cpuCostFactor},
	"st_force4d":                  {C: 100 * cpuCostFactor},
	"st_forcecollection":          {C: 100 * cpuCostFactor},
	"st_forcecurve":               {C: 1000 * cpuCostFactor},
	"st_forcepolygonccw":          {C: 100 * cpuCostFactor},
	"st_forcepolygoncw":           {C: 1000 * cpuCostFactor},
	"st_forcerhr":                 {C: 1000 * cpuCostFactor},
	"st_forcesfs":                 {C: 1000 * cpuCostFactor},
	"st_frechetdistance":          {C: 10000 * cpuCostFactor},
	"st_generatepoints":           {C: 10000 * cpuCostFactor},
	"st_geogfromtext":             {C: 100 * cpuCostFactor},
	"st_geogfromwkb":              {C: 100 * cpuCostFactor},
	"st_geographyfromtext":        {C: 100 * cpuCostFactor},
	"st_geohash":                  {C: 1000 * cpuCostFactor},
	"st_geomcollfromtext":         {C: 100 * cpuCostFactor},
	"st_geomcollfromwkb":          {C: 100 * cpuCostFactor},
	"st_geometricmedian":          {C: 10000 * cpuCostFactor},
	"st_geometryfromtext":         {C: 1000 * cpuCostFactor},
	"st_geometryn":                {C: 100 * cpuCostFactor},
	"st_geometrytype":             {C: 100 * cpuCostFactor},
	"st_geomfromewkb":             {C: 100 * cpuCostFactor},
	"st_geomfromewkt":             {C: 100 * cpuCostFactor},
	"st_geomfromgeohash":          {C: 1000 * cpuCostFactor},
	"st_geomfromgeojson":          {C: 1000 * cpuCostFactor},
	"st_geomfromgml":              {C: 100 * cpuCostFactor},
	"st_geomfromkml":              {C: 1000 * cpuCostFactor},
	"st_geomfromtext":             {C: 1000 * cpuCostFactor},
	"st_geomfromtwkb":             {C: 100 * cpuCostFactor},
	"st_geomfromwkb":              {C: 100 * cpuCostFactor},
	"st_gmltosql":                 {C: 100 * cpuCostFactor},
	"st_hasarc":                   {C: 100 * cpuCostFactor},
	"st_hausdorffdistance":        {C: 10000 * cpuCostFactor},
	"st_inittopogeo":              {C: 100 * cpuCostFactor},
	"st_interiorringn":            {C: 100 * cpuCostFactor},
	"st_interpolatepoint":         {C: 1000 * cpuCostFactor},
	"st_intersection":             {C: 100 * cpuCostFactor},
	"st_intersects":               {C: 100 * cpuCostFactor},
	"st_isclosed":                 {C: 100 * cpuCostFactor},
	"st_iscollection":             {C: 1000 * cpuCostFactor},
	"st_isempty":                  {C: 100 * cpuCostFactor},
	"st_ispolygonccw":             {C: 100 * cpuCostFactor},
	"st_ispolygoncw":              {C: 100 * cpuCostFactor},
	"st_isring":                   {C: 1000 * cpuCostFactor},
	"st_issimple":                 {C: 1000 * cpuCostFactor},
	"st_isvalid":                  {C: 100 * cpuCostFactor},
	"st_isvaliddetail":            {C: 10000 * cpuCostFactor},
	"st_isvalidreason":            {C: 100 * cpuCostFactor},
	"st_isvalidtrajectory":        {C: 10000 * cpuCostFactor},
	"st_length":                   {C: 100 * cpuCostFactor},
	"st_length2d":                 {C: 100 * cpuCostFactor},
	"st_length2dspheroid":         {C: 1000 * cpuCostFactor},
	"st_lengthspheroid":           {C: 1000 * cpuCostFactor},
	"st_linecrossingdirection":    {C: 10000 * cpuCostFactor},
	"st_linefromencodedpolyline":  {C: 1000 * cpuCostFactor},
	"st_linefrommultipoint":       {C: 100 * cpuCostFactor},
	"st_linefromtext":             {C: 100 * cpuCostFactor},
	"st_linefromwkb":              {C: 100 * cpuCostFactor},
	"st_lineinterpolatepoint":     {C: 1000 * cpuCostFactor},
	"st_lineinterpolatepoints":    {C: 1000 * cpuCostFactor},
	"st_linelocatepoint":          {C: 1000 * cpuCostFactor},
	"st_linemerge":                {C: 10000 * cpuCostFactor},
	"st_linestringfromwkb":        {C: 100 * cpuCostFactor},
	"st_linesubstring":            {C: 1000 * cpuCostFactor},
	"st_linetocurve":              {C: 10000 * cpuCostFactor},
	"st_locatealong":              {C: 1000 * cpuCostFactor},
	"st_locatebetween":            {C: 1000 * cpuCostFactor},
	"st_locatebetweenelevations":  {C: 1000 * cpuCostFactor},
	"st_longestline":              {C: 100 * cpuCostFactor},
	"st_makeenvelope":             {C: 100 * cpuCostFactor},
	"st_makeline":                 {C: 100 * cpuCostFactor},
	"st_makepoint":                {C: 100 * cpuCostFactor},
	"st_makepointm":               {C: 100 * cpuCostFactor},
	"st_makepolygon":              {C: 100 * cpuCostFactor},
	"st_makevalid":                {C: 10000 * cpuCostFactor},
	"st_maxdistance":              {C: 100 * cpuCostFactor},
	"st_memsize":                  {C: 100 * cpuCostFactor},
	"st_minimumboundingcircle":    {C: 10000 * cpuCostFactor},
	"st_minimumboundingradius":    {C: 10000 * cpuCostFactor},
	"st_minimumclearance":         {C: 10000 * cpuCostFactor},
	"st_minimumclearanceline":     {C: 10000 * cpuCostFactor},
	"st_mlinefromtext":            {C: 100 * cpuCostFactor},
	"st_mlinefromwkb":             {C: 100 * cpuCostFactor},
	"st_mpointfromtext":           {C: 100 * cpuCostFactor},
	"st_mpointfromwkb":            {C: 100 * cpuCostFactor},
	"st_mpolyfromtext":            {C: 100 * cpuCostFactor},
	"st_mpolyfromwkb":             {C: 100 * cpuCostFactor},
	"st_multi":                    {C: 100 * cpuCostFactor},
	"st_multilinefromwkb":         {C: 100 * cpuCostFactor},
	"st_multilinestringfromtext":  {C: 100 * cpuCostFactor},
	"st_multipointfromtext":       {C: 100 * cpuCostFactor},
	"st_multipointfromwkb":        {C: 100 * cpuCostFactor},
	"st_multipolyfromwkb":         {C: 100 * cpuCostFactor},
	"st_multipolygonfromtext":     {C: 100 * cpuCostFactor},
	"st_node":                     {C: 10000 * cpuCostFactor},
	"st_normalize":                {C: 100 * cpuCostFactor},
	"st_npoints":                  {C: 100 * cpuCostFactor},
	"st_nrings":                   {C: 100 * cpuCostFactor},
	"st_numgeometries":            {C: 100 * cpuCostFactor},
	"st_numinteriorring":          {C: 100 * cpuCostFactor},
	"st_numinteriorrings":         {C: 100 * cpuCostFactor},
	"st_numpatches":               {C: 100 * cpuCostFactor},
	"st_numpoints":                {C: 100 * cpuCostFactor},
	"st_offsetcurve":              {C: 10000 * cpuCostFactor},
	"st_orderingequals":           {C: 10000 * cpuCostFactor},
	"st_orientedenvelope":         {C: 10000 * cpuCostFactor},
	"st_overlaps":                 {C: 10000 * cpuCostFactor},
	"st_patchn":                   {C: 100 * cpuCostFactor},
	"st_perimeter":                {C: 100 * cpuCostFactor},
	"st_perimeter2d":              {C: 100 * cpuCostFactor},
	"st_point":                    {C: 100 * cpuCostFactor},
	"st_pointfromgeohash":         {C: 1000 * cpuCostFactor},
	"st_pointfromtext":            {C: 100 * cpuCostFactor},
	"st_pointfromwkb":             {C: 100 * cpuCostFactor},
	"st_pointinsidecircle":        {C: 1000 * cpuCostFactor},
	"st_pointn":                   {C: 100 * cpuCostFactor},
	"st_pointonsurface":           {C: 1000 * cpuCostFactor},
	"st_points":                   {C: 1000 * cpuCostFactor},
	"st_polyfromtext":             {C: 100 * cpuCostFactor},
	"st_polyfromwkb":              {C: 100 * cpuCostFactor},
	"st_polygon":                  {C: 100 * cpuCostFactor},
	"st_polygonfromtext":          {C: 100 * cpuCostFactor},
	"st_polygonfromwkb":           {C: 100 * cpuCostFactor},
	"st_polygonize":               {C: 10000 * cpuCostFactor},
	"st_project":                  {C: 1000 * cpuCostFactor},
	"st_quantizecoordinates":      {C: 1000 * cpuCostFactor},
	"st_relate":                   {C: 10000 * cpuCostFactor},
	"st_relatematch":              {C: 1000 * cpuCostFactor},
	"st_removepoint":              {C: 100 * cpuCostFactor},
	"st_removerepeatedpoints":     {C: 1000 * cpuCostFactor},
	"st_reverse":                  {C: 1000 * cpuCostFactor},
	"st_rotate":                   {C: 100 * cpuCostFactor},
	"st_rotatex":                  {C: 100 * cpuCostFactor},
	"st_rotatey":                  {C: 100 * cpuCostFactor},
	"st_rotatez":                  {C: 100 * cpuCostFactor},
	"st_scale":                    {C: 100 * cpuCostFactor},
	"st_segmentize":               {C: 1000 * cpuCostFactor},
	"st_seteffectivearea":         {C: 1000 * cpuCostFactor},
	"st_setpoint":                 {C: 100 * cpuCostFactor},
	"st_setsrid":                  {C: 100 * cpuCostFactor},
	"st_sharedpaths":              {C: 10000 * cpuCostFactor},
	"st_shortestline":             {C: 1000 * cpuCostFactor},
	"st_simplify":                 {C: 100 * cpuCostFactor},
	"st_simplifypreservetopology": {C: 10000 * cpuCostFactor},
	"st_simplifyvw":               {C: 10000 * cpuCostFactor},
	"st_snap":                     {C: 10000 * cpuCostFactor},
	"st_snaptogrid":               {C: 100 * cpuCostFactor},
	"st_split":                    {C: 10000 * cpuCostFactor},
	"st_srid":                     {C: 100 * cpuCostFactor},
	"st_startpoint":               {C: 100 * cpuCostFactor},
	"st_subdivide":                {C: 10000 * cpuCostFactor},
	"st_summary":                  {C: 100 * cpuCostFactor},
	"st_swapordinates":            {C: 100 * cpuCostFactor},
	"st_symdifference":            {C: 10000 * cpuCostFactor},
	"st_symmetricdifference":      {C: 10000 * cpuCostFactor},
	"st_tileenvelope":             {C: 100 * cpuCostFactor},
	"st_touches":                  {C: 10000 * cpuCostFactor},
	"st_transform":                {C: 100 * cpuCostFactor},
	"st_translate":                {C: 100 * cpuCostFactor},
	"st_transscale":               {C: 100 * cpuCostFactor},
	"st_unaryunion":               {C: 10000 * cpuCostFactor},
	"st_union":                    {C: 10000 * cpuCostFactor},
	"st_voronoilines":             {C: 100 * cpuCostFactor},
	"st_voronoipolygons":          {C: 100 * cpuCostFactor},
	"st_within":                   {C: 10000 * cpuCostFactor},
	"st_wkbtosql":                 {C: 100 * cpuCostFactor},
	"st_wkttosql":                 {C: 1000 * cpuCostFactor},
}

// Init initializes a new coster structure with the given memo.
func (c *coster) Init(
	ctx context.Context,
	evalCtx *eval.Context,
	mem *memo.Memo,
	perturbation float64,
	rng *rand.Rand,
	o *Optimizer,
) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = coster{
		ctx:          ctx,
		evalCtx:      evalCtx,
		mem:          mem,
		locality:     evalCtx.Locality,
		perturbation: perturbation,
		rng:          rng,
		o:            o,
	}
}

// MaybeGetBestCostRelation is part of the xform.Coster interface.
func (c *coster) MaybeGetBestCostRelation(
	grp memo.RelExpr, required *physical.Required,
) (best memo.RelExpr, ok bool) {
	return c.o.MaybeGetBestCostRelation(grp, required)
}

// ComputeCost calculates the estimated cost of the top-level operator in a
// candidate best expression, based on its logical properties and those of its
// children.
//
// Note: each custom function to compute the cost of an operator calculates
// the cost based on Big-O estimated complexity. Most constant factors are
// ignored for now.
func (c *coster) ComputeCost(candidate memo.RelExpr, required *physical.Required) memo.Cost {
	opt.MaybeInjectOptimizerTestingPanic(c.ctx, c.evalCtx)

	var cost memo.Cost
	switch candidate.Op() {
	case opt.TopKOp:
		cost = c.computeTopKCost(candidate.(*memo.TopKExpr), required)

	case opt.SortOp:
		cost = c.computeSortCost(candidate.(*memo.SortExpr), required)

	case opt.DistributeOp:
		cost = c.computeDistributeCost(candidate.(*memo.DistributeExpr), required)

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
		cost = c.computeSetCost(candidate, required)

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

	case opt.VectorSearchOp:
		cost = c.computeVectorSearchCost(candidate.(*memo.VectorSearchExpr))

	case opt.VectorMutationSearchOp:
		cost = c.computeVectorMutationSearchCost(candidate.(*memo.VectorMutationSearchExpr))

	case opt.InsertOp:
		insertExpr, _ := candidate.(*memo.InsertExpr)
		if len(insertExpr.FastPathUniqueChecks) != 0 {
			if len(insertExpr.FastPathUniqueChecks[0].DatumsFromConstraint) != 0 {
				// Make the cost of insert fast path slightly cheaper than non-fast path
				// so that the optimizer will pick it. All of the costed operations
				// should have identical costs between the two inserts.
				cost.C -= cpuCostFactor
			}
		}

	case opt.ExplainOp:
		// Technically, the cost of an Explain operation is independent of the cost
		// of the underlying plan. However, we want to explain the plan we would get
		// without EXPLAIN, i.e. the lowest cost plan. So do nothing special to get
		// default behavior.
	}

	// Add a one-time cost for any operator, meant to reflect the cost of setting
	// up execution for the operator. This makes plans with fewer operators
	// preferable, all else being equal.
	cost.C += cpuCostFactor

	// Add a one-time cost for any operator with unbounded cardinality. This
	// ensures we prefer plans that push limits as far down the tree as possible,
	// all else being equal.
	//
	// Also add a cost flag for unbounded cardinality.
	if candidate.Relational().Cardinality.IsUnbounded() {
		cost.C += cpuCostFactor
		if c.evalCtx.SessionData().OptimizerPreferBoundedCardinality {
			cost.Flags.UnboundedCardinality = true
		}
	}

	if !cost.Less(memo.MaxCost) {
		// Optsteps uses MaxCost to suppress nodes in the memo. When a node with
		// MaxCost is added to the memo, it can lead to an obscure crash with an
		// unknown node. We'd rather detect this early.
		panic(errors.AssertionFailedf("node %s with MaxCost added to the memo", redact.Safe(candidate.Op())))
	}

	if c.perturbation != 0 {
		// Don't perturb the cost if we are forcing an index.
		if cost.Less(hugeCost) {
			// Get a random value in the range [-1.0, 1.0)
			var multiplier float64
			if c.rng == nil {
				multiplier = 2*rand.Float64() - 1
			} else {
				multiplier = 2*c.rng.Float64() - 1
			}

			// If perturbation is p, and the estimated cost of an expression is c,
			// the new cost is in the range [max(0, c - pc), c + pc). For example,
			// if p=1.5, the new cost is in the range [0, c + 1.5 * c).
			perturbedCost := cost.C * (1 + c.perturbation*multiplier)
			cost.C = math.Max(0, perturbedCost)
		}
	}

	return cost
}

func (c *coster) computeTopKCost(topk *memo.TopKExpr, required *physical.Required) memo.Cost {
	rel := topk.Relational()
	outputRowCount := rel.Statistics().RowCount

	inputRowCount := topk.Input.Relational().Statistics().RowCount
	if !required.Ordering.Any() {
		// When there is a partial ordering of the input rows' sort columns, we may
		// be able to reduce the number of input rows needed to find the top K rows.
		inputRowCount = topKInputLimitHint(c.mem, topk, inputRowCount, outputRowCount, float64(topk.K))
	}
	// Add the cost of sorting.
	// Start with a cost of storing each row; TopK sort only stores K rows in a
	// max heap.
	cost := memo.Cost{C: cpuCostFactor * float64(rel.OutputCols.Len()) * outputRowCount}

	// Add buffering cost for the output rows.
	cost.Add(c.rowBufferCost(outputRowCount))

	// In the worst case, there are O(N*log(K)) comparisons to compare each row in
	// the input to the top of the max heap and sift the max heap if each row
	// compared is in the top K found so far.
	cost.C += c.rowCmpCost(len(topk.Ordering.Columns)).C * (1 + math.Log2(math.Max(outputRowCount, 1))) * inputRowCount

	// TODO(harding): Add the CPU cost of emitting the K output rows. This should
	// be done in conjunction with computeSortCost.

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
	stats := rel.Statistics()
	numSegments := c.countSegments(sort)

	// Start with a cost of storing each row; this takes the total number of
	// columns into account so that a sort on fewer columns is preferred (e.g.
	// sort before projecting a new column).
	cost := memo.Cost{C: cpuCostFactor * float64(rel.OutputCols.Len()) * stats.RowCount}

	if !sort.InputOrdering.Any() {
		// Add the cost for finding the segments: each row is compared to the
		// previous row on the preordered columns. Most of these comparisons will
		// yield equality, so we don't use rowCmpCost(): we expect to have to
		// compare all preordered columns.
		cost.C += cpuCostFactor * float64(numPreorderedCols) * stats.RowCount
	}

	// Add the cost to sort the segments. On average, each row is involved in
	// O(log(segmentSize)) comparisons.
	numCmpOpsPerRow := float64(1)
	if segmentSize := stats.RowCount / numSegments; segmentSize > 1 {
		numCmpOpsPerRow += math.Log2(segmentSize)

		// Add a cost for buffering rows that takes into account increased memory
		// pressure and the possibility of spilling to disk.
		cost.C += numSegments * c.rowBufferCost(segmentSize).C
	}
	cost.C += c.rowCmpCost(numKeyCols-numPreorderedCols).C * numCmpOpsPerRow * stats.RowCount
	// TODO(harding): Add the CPU cost of emitting the output rows. This should be
	// done in conjunction with computeTopKCost.
	return cost
}

func (c *coster) computeDistributeCost(
	distribute *memo.DistributeExpr, required *physical.Required,
) memo.Cost {
	if distribute.NoOpDistribution() {
		// If the distribution will be elided, the cost is zero.
		return memo.Cost{C: 0}
	}
	if target, source, ok := distribute.GetDistributions(); ok {
		if distributionIsLocal(target, c.evalCtx) {
			return c.distributionCost(source)
		}
	}
	if c.evalCtx != nil && c.evalCtx.Planner.EnforceHomeRegion() {
		if distribute.HasHomeRegion() {
			// Query plans with a home region are favored over those without one.
			return LargeDistributeCostWithHomeRegion
		}
		return LargeDistributeCost
	}

	// TODO(rytaft,msirek): Compute a real cost here. Currently this is a rough
	//                      estimate of latency overhead, but actual measurements
	//                      would be useful.
	return DistributeCost
}

func (c *coster) computeVectorSearchCost(search *memo.VectorSearchExpr) memo.Cost {
	// TODO(drewk, mw5h): implement a proper cost function.
	return memo.Cost{C: cpuCostFactor * search.Relational().Statistics().RowCount}
}

func (c *coster) computeVectorMutationSearchCost(search *memo.VectorMutationSearchExpr) memo.Cost {
	// TODO(drewk, mw5h): implement a proper cost function.
	return memo.Cost{C: cpuCostFactor * search.Relational().Statistics().RowCount}
}

func (c *coster) computeScanCost(scan *memo.ScanExpr, required *physical.Required) memo.Cost {
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index || scan.Flags.ForceZigzag {
		// If we are forcing an index, any other index has a very high cost. In
		// practice, this will only happen when this is a primary index scan.
		return hugeCost
	}

	isUnfiltered := scan.IsUnfiltered(c.mem.Metadata())

	// Normally a full scan of a partial index would not be considered a "full
	// scan" for the purposes of the NO_FULL_SCAN and AVOID_FULL_SCAN hints
	// (isUnfiltered is false for partial indexes), but if the user has explicitly
	// forced the partial index, we do consider it a full scan.
	isFullScan := isUnfiltered || (scan.Flags.ForceIndex && scan.IsFullIndexScan())
	if scan.Flags.NoFullScan && isFullScan {
		return hugeCost
	}

	if scan.Flags.ForceInvertedIndex && !scan.IsInvertedScan(c.mem.Metadata()) {
		return hugeCost
	}

	stats := scan.Relational().Statistics()
	rowCount := stats.RowCount
	if isUnfiltered && c.evalCtx != nil && c.evalCtx.SessionData().DisallowFullTableScans &&
		!c.evalCtx.SessionData().Internal {
		if !scan.IsVirtualTable(c.mem.Metadata()) {
			// Don't apply the huge cost to full scans of virtual tables since
			// we don't reject them anyway. In other words, we would only
			// penalize plans with full scans of virtual tables, which might
			// force us to choose a plan that is actually worse but doesn't get
			// the huge cost since it doesn't contain a full scan (e.g. we could
			// do a virtual table lookup join instead).
			// TODO(#123783): once we start rejecting plans with full scans of
			// virtual tables, we should apply the cost penalty here.
			isLarge := !stats.Available || rowCount > c.evalCtx.SessionData().LargeFullScanRows
			if isLarge {
				return hugeCost
			}
		}
	}

	// Add the IO cost of retrieving and the CPU cost of emitting the rows. The
	// row cost depends on the size of the columns scanned.
	perRowCost := c.rowScanCost(scan.Table, scan.Index, scan.Cols)

	numSpans := 1
	if scan.Constraint != nil {
		numSpans = scan.Constraint.Spans.Count()
	} else if scan.InvertedConstraint != nil {
		numSpans = len(scan.InvertedConstraint)
	}
	baseCost := memo.Cost{C: float64(numSpans) * randIOCostFactor}

	// If this is a virtual scan, add the cost of fetching table descriptors.
	if c.mem.Metadata().Table(scan.Table).IsVirtualTable() {
		baseCost.C += virtualScanTableDescriptorFetchCost
	}

	// Performing a reverse scan is more expensive than a forward scan, but it's
	// still preferable to sorting the output of a forward scan. To ensure we
	// choose a reverse scan over a sort, add the reverse scan cost before we
	// alter the row count for unbounded scan penalties below. This cost must also
	// be added before adjusting the row count for the limit hint.
	if ordering.ScanIsReverse(c.mem, scan, &required.Ordering) {
		if rowCount > 1 {
			// Need to do binary search to seek to the previous row.
			perRowCost.C += math.Log2(rowCount) * cpuCostFactor
		}
	}

	// Add a penalty to full table scans. All else being equal, we prefer a
	// constrained scan. Adding a few rows worth of cost helps prevent surprising
	// plans for very small tables.
	if isUnfiltered {
		rowCount += fullScanRowCountPenalty

		// For tables with multiple partitions, add the cost of visiting each
		// partition.
		// TODO(rytaft): In the future we should take latency into account here.
		index := c.mem.Metadata().Table(scan.Table).Index(scan.Index)
		if partitionCount := index.PartitionCount(); partitionCount > 1 {
			// Subtract 1 since we already accounted for the first partition when
			// counting spans.
			baseCost.C += float64(partitionCount-1) * randIOCostFactor
		}
	}

	// Add a penalty if the cardinality exceeds the row count estimate. Adding a
	// few rows worth of cost helps prevent surprising plans for very small tables
	// or for when stats are stale.
	//
	// Note: we add this to the baseCost rather than the rowCount, so that the
	// number of index columns does not have an outsized effect on the cost of
	// the scan. See issue #68556.
	baseCost.Add(c.largeCardinalityCostPenalty(scan.Relational().Cardinality, rowCount))

	if required.LimitHint != 0 {
		rowCount = math.Min(rowCount, required.LimitHint)
	}

	cost := baseCost
	cost.C += rowCount * (seqIOCostFactor + perRowCost.C)

	var regionsAccessed physical.Distribution
	if scan.Distribution.Regions != nil {
		regionsAccessed = scan.Distribution
	} else {
		tabMeta := c.mem.Metadata().TableMeta(scan.Table)
		regionsAccessed.FromIndexScan(c.ctx, c.evalCtx, tabMeta, scan.Index, scan.Constraint)
	}
	if scan.LocalityOptimized {
		return cost
	}
	extraCost := c.distributionCost(regionsAccessed)
	cost.Add(extraCost)

	// Apply a penalty for a full scan if needed.
	if scan.Flags.AvoidFullScan && isFullScan {
		cost.Flags.FullScanPenalty = true
	}

	return cost
}

func distributionIsLocal(regionsAccessed physical.Distribution, evalCtx *eval.Context) bool {
	if len(regionsAccessed.Regions) == 1 {
		var localDist physical.Distribution
		localDist.FromLocality(evalCtx.Locality)
		return localDist.Equals(regionsAccessed)
	}
	return false
}

// distributionCost returns the cost to perform a distribution from
// `regionsAccessed` to the gateway region.
func (c *coster) distributionCost(regionsAccessed physical.Distribution) (cost memo.Cost) {
	if len(regionsAccessed.Regions) == 1 {
		var localDist physical.Distribution
		localDist.FromLocality(c.evalCtx.Locality)
		if localDist.Equals(regionsAccessed) {
			// Operations accessing only the local region don't have a remote latency cost.
			return cost
		}
	}
	// Operations that read rows outside of the gateway region incur a
	// distribution cost.
	// TODO(rytaft,msirek): Compute a real cost here. Currently this is a rough
	//                      estimate of latency overhead, but actual measurements
	//                      would be useful.
	extraCost := DistributeCost
	if regionsAccessed.Any() {
		// Non-multiregion tables may have no regions populated in regionsAccessed.
		// To avoid potential plan regressions involving non-multiregion tables,
		// don't add the somewhat large `DistributeCost` when
		// `regionsAccessed.Any()` is true because query planning can't be done in
		// that case to try and avoid the distribution anyway.
		extraCost = SmallDistributeCost
	} else if !regionsAccessed.Any() && c.evalCtx != nil &&
		c.evalCtx.Planner.EnforceHomeRegion() {
		if len(regionsAccessed.Regions) == 1 {
			// Query plans with a home region are favored over those without one.
			extraCost = LargeDistributeCostWithHomeRegion
		} else {
			extraCost = LargeDistributeCost
		}
	}
	return extraCost
}

func (c *coster) computeSelectCost(sel *memo.SelectExpr, required *physical.Required) memo.Cost {
	// Typically the filter has to be evaluated on each input row.
	inputRowCount := sel.Input.Relational().Statistics().RowCount

	// If there is a LimitHint, n, it is expected that the filter will only be
	// evaluated on the number of rows required to produce n rows.
	if required.LimitHint != 0 {
		selectivity := sel.Relational().Statistics().Selectivity.AsFloat()
		inputRowCount = math.Min(inputRowCount, required.LimitHint/selectivity)
	}

	filterSetup, filterPerRow := c.computeFiltersCost(sel.Filters, intsets.Fast{})
	cost := memo.Cost{C: inputRowCount * filterPerRow.C}
	cost.Add(filterSetup)
	return cost
}

func (c *coster) computeProjectCost(prj *memo.ProjectExpr) memo.Cost {
	// Each synthesized column causes an expression to be evaluated on each row.
	rowCount := prj.Relational().Statistics().RowCount
	synthesizedColCount := len(prj.Projections)
	cost := memo.Cost{C: rowCount * float64(synthesizedColCount) * cpuCostFactor}

	// Add the CPU cost of emitting the rows.
	cost.C += rowCount * cpuCostFactor
	return cost
}

func (c *coster) computeInvertedFilterCost(invFilter *memo.InvertedFilterExpr) memo.Cost {
	// The filter has to be evaluated on each input row.
	inputRowCount := invFilter.Input.Relational().Statistics().RowCount
	cost := memo.Cost{C: inputRowCount * cpuCostFactor}
	return cost
}

func (c *coster) computeValuesCost(values *memo.ValuesExpr) memo.Cost {
	return memo.Cost{C: values.Relational().Statistics().RowCount * cpuCostFactor}
}

func (c *coster) computeHashJoinCost(join memo.RelExpr) memo.Cost {
	if join.Private().(*memo.JoinPrivate).Flags.Has(memo.DisallowHashJoinStoreRight) {
		return hugeCost
	}
	leftRowCount := join.Child(0).(memo.RelExpr).Relational().Statistics().RowCount
	rightRowCount := join.Child(1).(memo.RelExpr).Relational().Statistics().RowCount
	if (join.Op() == opt.SemiJoinOp || join.Op() == opt.AntiJoinOp) && leftRowCount < rightRowCount {
		// The execution engine always builds the hash table on the right side
		// of the join, so it is beneficial for the smaller relation to be on
		// the right. This decision is made in execbuilder because the optimizer
		// does not have an expressions representing right semi and anti joins,
		// it only has SemiJoin and AntiJoin. We swap row counts here to mimic
		// the decision made in execbuilder and generate an accurate cost.
		//
		// There is no need to consider join hints here because there is no way
		// to apply join hints to semi or anti joins. Join hints are only
		// possible on explicit joins using the JOIN keyword, and semi and anti
		// joins are only created from implicit joins without the JOIN keyword.
		leftRowCount, rightRowCount = rightRowCount, leftRowCount
	}

	// A hash join must process every row from both tables once.
	//
	// We add some factors to account for the hashtable build and lookups. The
	// right side is the one stored in the hashtable, so we use a larger factor
	// for that side. This ensures that a join with the smaller right side is
	// preferred to the symmetric join.
	cost := memo.Cost{C: (1.25*leftRowCount + 1.75*rightRowCount) * cpuCostFactor}

	// Add a cost for buffering rows that takes into account increased memory
	// pressure and the possibility of spilling to disk.
	cost.Add(c.rowBufferCost(rightRowCount))

	// Compute filter cost. Fetch the indices of the filters that will be used in
	// the join, since they will not add to the cost and should be skipped.
	on := join.Child(2).(*memo.FiltersExpr)
	leftCols := join.Child(0).(memo.RelExpr).Relational().OutputCols
	rightCols := join.Child(1).(memo.RelExpr).Relational().OutputCols
	filtersToSkip := memo.ExtractJoinConditionFilterOrds(leftCols, rightCols, *on, false /* inequality */)
	filterSetup, filterPerRow := c.computeFiltersCost(*on, filtersToSkip)
	cost.Add(filterSetup)

	// Add the CPU cost of emitting the rows.
	rowsProcessed, ok := c.mem.RowsProcessed(join)
	if !ok {
		// This can happen as part of testing. In this case just return the number
		// of rows.
		rowsProcessed = join.Relational().Statistics().RowCount
	}
	cost.C += rowsProcessed * filterPerRow.C

	return cost
}

func (c *coster) computeMergeJoinCost(join *memo.MergeJoinExpr) memo.Cost {
	if join.MergeJoinPrivate.Flags.Has(memo.DisallowMergeJoin) {
		return hugeCost
	}
	leftRowCount := join.Left.Relational().Statistics().RowCount
	rightRowCount := join.Right.Relational().Statistics().RowCount

	if (join.Op() == opt.SemiJoinOp || join.Op() == opt.AntiJoinOp) && leftRowCount < rightRowCount {
		// If we have a semi or an anti join, during the execbuilding we choose
		// the relation with smaller cardinality to be on the right side, so we
		// need to swap row counts accordingly.
		// TODO(raduberinde): we might also need to look at memo.JoinFlags when
		// choosing a side.
		leftRowCount, rightRowCount = rightRowCount, leftRowCount
	}

	// The vectorized merge join in some cases buffers rows from the right side
	// whereas the left side is processed in a streaming fashion. To account for
	// this difference, we multiply both row counts so that a join with the
	// smaller right side is preferred to the symmetric join.
	cost := memo.Cost{C: (0.9*leftRowCount + 1.1*rightRowCount) * cpuCostFactor}

	filterSetup, filterPerRow := c.computeFiltersCost(join.On, intsets.Fast{})
	cost.Add(filterSetup)

	// Add the CPU cost of emitting the rows.
	rowsProcessed, ok := c.mem.RowsProcessed(join)
	if !ok {
		// We shouldn't ever get here. Since we don't allow the memo
		// to be optimized twice, the coster should never be used after
		// logPropsBuilder.clear() is called.
		panic(errors.AssertionFailedf("could not get rows processed for merge join"))
	}
	cost.C += rowsProcessed * filterPerRow.C
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
	cost := c.computeIndexLookupJoinCost(
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
	_, provided := distribution.BuildLookupJoinLookupTableDistribution(
		c.ctx, c.evalCtx, c.mem, join, required, c.MaybeGetBestCostRelation)
	extraCost := c.distributionCost(provided)
	cost.Add(extraCost)
	return cost
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
	lookupCount := input.Relational().Statistics().RowCount

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
		outputRows := join.Relational().Statistics().RowCount
		unlimitedLookupCount := lookupCount
		lookupCount = lookupJoinInputLimitHint(unlimitedLookupCount, outputRows, required.LimitHint)
		// We scale the number of rows processed by the same factor (we are
		// calculating the average number of rows processed per lookup and
		// multiplying by the new lookup count).
		rowsProcessed = (rowsProcessed / unlimitedLookupCount) * lookupCount
	}

	perLookupCost := indexLookupJoinPerLookupCost(join)
	if !lookupColsAreTableKey {
		// If the lookup columns don't form a key, execution will have to limit
		// KV batches which prevents running requests to multiple nodes in parallel.
		// An experiment on a 4 node cluster with a table with 100k rows split into
		// 100 ranges showed that a "non-parallel" lookup join is about 5 times
		// slower.
		// TODO(drewk): this no longer applies now that the streamer work is used.
		perLookupCost.C += 4 * randIOCostFactor
	}
	if c.mem.Metadata().Table(table).IsVirtualTable() {
		// It's expensive to perform a lookup join into a virtual table because
		// we need to fetch the table descriptors on each lookup.
		perLookupCost.C += virtualScanTableDescriptorFetchCost
	}
	cost := memo.Cost{C: lookupCount * perLookupCost.C}

	filterSetup, filterPerRow := c.computeFiltersCost(on, intsets.Fast{})
	cost.Add(filterSetup)

	// Each lookup might retrieve many rows; add the IO cost of retrieving the
	// rows (relevant when we expect many resulting rows per lookup) and the CPU
	// cost of emitting the rows.
	// TODO(harding): Add the cost of reading all columns in the lookup table when
	// we cost rows by column size.
	lookupCols := cols.Difference(input.Relational().OutputCols)
	perRowCost := lookupJoinRetrieveRowCost + filterPerRow.C +
		c.rowScanCost(table, index, lookupCols).C

	cost.C += rowsProcessed * perRowCost

	if flags.Has(memo.PreferLookupJoinIntoRight) {
		// If we prefer a lookup join, make the cost much smaller.
		cost.C *= preferLookupJoinFactor
	}

	// If this lookup join is locality optimized, divide the cost by 2.5 in order to make
	// the total cost of the two lookup joins in the locality optimized plan less than
	// the cost of the single lookup join in the non-locality optimized plan.
	// TODO(rytaft): This is hacky. We should really be making this determination
	// based on the latency between regions.
	if localityOptimized {
		cost.C /= 2.5
	}
	return cost
}

func (c *coster) computeInvertedJoinCost(
	join *memo.InvertedJoinExpr, required *physical.Required,
) memo.Cost {
	if join.InvertedJoinPrivate.Flags.Has(memo.DisallowInvertedJoinIntoRight) {
		return hugeCost
	}
	lookupCount := join.Input.Relational().Statistics().RowCount

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
		outputRows := join.Relational().Statistics().RowCount
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
	perLookupCost := memo.Cost{C: randIOCostFactor}
	// Since inverted indexes can't form a key, execution will have to
	// limit KV batches which prevents running requests to multiple nodes
	// in parallel.  An experiment on a 4 node cluster with a table with
	// 100k rows split into 100 ranges showed that a "non-parallel" lookup
	// join is about 5 times slower.
	perLookupCost.C *= 5
	cost := memo.Cost{C: lookupCount * perLookupCost.C}

	filterSetup, filterPerRow := c.computeFiltersCost(join.On, intsets.Fast{})
	cost.Add(filterSetup)

	// Each lookup might retrieve many rows; add the IO cost of retrieving the
	// rows (relevant when we expect many resulting rows per lookup) and the CPU
	// cost of emitting the rows.
	lookupCols := join.Cols.Difference(join.Input.Relational().OutputCols)
	perRowCost := memo.Cost{C: lookupJoinRetrieveRowCost + filterPerRow.C +
		c.rowScanCost(join.Table, join.Index, lookupCols).C}

	cost.C += rowsProcessed * perRowCost.C

	provided := distribution.BuildInvertedJoinLookupTableDistribution(c.ctx, c.evalCtx, c.mem, join)
	extraCost := c.distributionCost(provided)
	cost.Add(extraCost)
	return cost
}

// computeExprCost calculates per-row cost of the expression.
// It finds every embedded spatial function and add its cost.
func (c *coster) computeExprCost(expr opt.Expr) memo.Cost {
	perRowCost := memo.Cost{C: 0}
	if expr.Op() == opt.FunctionOp {
		// We are ok with the zero value here for functions not in the map.
		function := expr.(*memo.FunctionExpr)
		perRowCost.Add(fnCost[function.Name])
	}
	// recurse into the children of the current expression
	for i := 0; i < expr.ChildCount(); i++ {
		perRowCost.Add(c.computeExprCost(expr.Child(i)))
	}
	return perRowCost
}

// computeFiltersCost returns the setup and per-row cost of executing
// a filter. Callers of this function should add setupCost and multiply
// perRowCost by the number of rows expected to be filtered.
//
// filtersToSkip identifies the indices of filters that should be skipped,
// because they do not add to the cost. This can happen when a condition still
// exists in the filters even though it is handled by the join.
func (c *coster) computeFiltersCost(
	filters memo.FiltersExpr, filtersToSkip intsets.Fast,
) (setupCost, perRowCost memo.Cost) {
	// Add a base perRowCost so that callers do not need to have their own
	// base per-row cost.
	perRowCost.C += cpuCostFactor
	for i := range filters {
		if filtersToSkip.Contains(i) {
			continue
		}
		f := &filters[i]
		perRowCost.Add(c.computeExprCost(f.Condition))
		// Add a constant "setup" cost per ON condition to account for the fact that
		// the rowsProcessed estimate alone cannot effectively discriminate between
		// plans when RowCount is too small.
		setupCost.C += cpuCostFactor
	}
	return setupCost, perRowCost
}

func (c *coster) computeZigzagJoinCost(join *memo.ZigzagJoinExpr) memo.Cost {
	rowCount := join.Relational().Statistics().RowCount

	// Assume the upper bound on scan cost to be the sum of the cost of scanning
	// the two constituent indexes. To determine which columns are returned from
	// each scan, intersect the output column set join.Cols with each side's
	// IndexColumns. Columns present in both indexes are projected from the left
	// side only.
	md := c.mem.Metadata()
	leftCols := md.TableMeta(join.LeftTable).IndexColumns(join.LeftIndex)
	leftCols.IntersectionWith(join.Cols)
	rightCols := md.TableMeta(join.RightTable).IndexColumns(join.RightIndex)
	rightCols.IntersectionWith(join.Cols)
	rightCols.DifferenceWith(leftCols)
	scanCost := c.rowScanCost(join.LeftTable, join.LeftIndex, leftCols)
	scanCost.Add(c.rowScanCost(join.RightTable, join.RightIndex, rightCols))

	filterSetup, filterPerRow := c.computeFiltersCost(join.On, intsets.Fast{})

	// It is much more expensive to do a seek in zigzag join vs. lookup join
	// because zigzag join starts a new scan for every seek via
	// `Fetcher.StartScan`. Instead of using `seqIOCostFactor`, bump seek costs to
	// be similar to lookup join, though more fine-tuning is needed.
	// TODO(msirek): Refine zigzag join costs and try out changes to execution to
	//               do a point lookup for a match in the other index before
	//               starting a new scan. Lookup join and inverted join add a
	//               cost of 5 * randIOCostFactor per row to account for not
	//               running non-key lookups in parallel. This may be applicable
	//               here too.
	//               Explore dynamically detecting selection of a bad zigzag join
	//               during execution and switching to merge join on-the-fly.
	// Seek costs should be at least as expensive as lookup join.
	// See `indexLookupJoinPerLookupCost` and `computeIndexLookupJoinCost`.
	// Increased zigzag join costs mean that accurate selectivity estimation is
	// needed to ensure this index access path can be picked.
	seekCost := memo.Cost{C: randIOCostFactor + lookupJoinRetrieveRowCost}

	// Double the cost of emitting rows as well as the cost of seeking rows,
	// given two indexes will be accessed.
	cost := memo.Cost{C: rowCount * (2*(cpuCostFactor+seekCost.C) + scanCost.C + filterPerRow.C)}
	cost.Add(filterSetup)

	// Add a penalty if the cardinality exceeds the row count estimate. Adding a
	// few rows worth of cost helps prevent surprising plans for very small tables
	// or for when stats are stale. This is also needed to ensure parity with the
	// cost of scans.
	//
	// Note: we add this directly to the cost rather than the rowCount, so that
	// the number of index columns does not have an outsized effect on the cost of
	// the zigzag join. See issue #68556.
	cost.Add(c.largeCardinalityCostPenalty(join.Relational().Cardinality, rowCount))

	if c.evalCtx != nil && c.evalCtx.SessionData().OptimizerUseImprovedZigzagJoinCosting {
		// Add one randIOCostFactor of additional seek cost so the cost is at least as
		// much as a scan if rowCount is less than one.
		cost.C += randIOCostFactor

		// TODO(rytaft): We don't capture distribution info in zigzag joins, so pass
		// an empty distribution. We need to add some distribution cost to prevent the
		// coster from always preferring zigzag joins over scans. If we ever want to
		// make zigzag joins a priority again, we should store a real distribution
		// value on the zigzag join, similar to scans.
		cost.Add(c.distributionCost(physical.Distribution{}))
	}

	return cost
}

// isStreamingSetOperator returns true if relation is a streaming set operator.
func isStreamingSetOperator(relation memo.RelExpr) bool {
	if opt.IsSetOp(relation) {
		return !relation.Private().(*memo.SetPrivate).Ordering.Any()
	}
	return false
}

func (c *coster) computeSetCost(set memo.RelExpr, required *physical.Required) memo.Cost {
	// Add the CPU cost of emitting the rows.
	outputRowCount := set.Relational().Statistics().RowCount
	if outputRowCount != 0 && required.LimitHint != 0 && outputRowCount > required.LimitHint {
		outputRowCount = required.LimitHint
	}
	cost := memo.Cost{C: outputRowCount * cpuCostFactor}

	// A set operation must process every row from both tables once. UnionAll and
	// LocalityOptimizedSearch can avoid any extra computation, but all other set
	// operations must perform a hash table lookup or update for each input row.
	//
	// The exception is if this is a streaming set operation, in which case there
	// is no need to build a hash table. We can detect that this is a streaming
	// operation by checking whether the ordering is defined in the set private
	// (see isStreamingSetOperator).
	if set.Op() != opt.UnionAllOp && set.Op() != opt.LocalityOptimizedSearchOp &&
		!isStreamingSetOperator(set) {
		leftRowCount := set.Child(0).(memo.RelExpr).Relational().Statistics().RowCount
		rightRowCount := set.Child(1).(memo.RelExpr).Relational().Statistics().RowCount
		cost.C += (leftRowCount + rightRowCount) * cpuCostFactor

		// Add a cost for buffering rows that takes into account increased memory
		// pressure and the possibility of spilling to disk.
		switch set.Op() {
		case opt.UnionOp:
			// Hash Union is implemented as UnionAll followed by Hash Distinct.
			cost.Add(c.rowBufferCost(outputRowCount))

		case opt.IntersectOp, opt.ExceptOp:
			// Hash Intersect and Except are implemented as Hash Distinct on each
			// input followed by a Hash Join that builds the hash table from the right
			// input.
			cost.Add(c.rowBufferCost(leftRowCount))
			rightCost := c.rowBufferCost(rightRowCount)
			cost.Add(rightCost)
			cost.Add(rightCost)

		case opt.IntersectAllOp, opt.ExceptAllOp:
			// Hash IntersectAll and ExceptAll are implemented as a Hash Join that
			// builds the hash table from the right input.
			cost.Add(c.rowBufferCost(rightRowCount))

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
	cost := memo.Cost{C: cpuCostFactor}

	// Add the CPU cost of emitting the rows.
	outputRowCount := grouping.Relational().Statistics().RowCount
	cost.C += outputRowCount * cpuCostFactor

	private := grouping.Private().(*memo.GroupingPrivate)
	groupingColCount := private.GroupingCols.Len()
	aggsCount := grouping.Child(1).ChildCount()

	// Normally, a grouping expression must process each input row once.
	inputRowCount := grouping.Child(0).(memo.RelExpr).Relational().Statistics().RowCount

	// If this is a streaming GroupBy or a DistinctOn with a limit hint, l, we
	// only need to process enough input rows to output l rows.
	streamingType := private.GroupingOrderType(&required.Ordering)
	if required.LimitHint > 0 {
		if grouping.Op() == opt.GroupByOp && streamingType != memo.NoStreaming {
			inputRowCount = streamingGroupByInputLimitHint(inputRowCount, outputRowCount, required.LimitHint)
			outputRowCount = math.Min(outputRowCount, required.LimitHint)
		} else if grouping.Op() == opt.DistinctOnOp &&
			c.evalCtx.SessionData().OptimizerUseImprovedDistinctOnLimitHintCosting {
			if d := distinctOnLimitHint(outputRowCount, required.LimitHint); d > 0 {
				inputRowCount = d
			}
			outputRowCount = math.Min(outputRowCount, required.LimitHint)
		}
	}

	// Cost per row depends on the number of grouping columns and the number of
	// aggregates.
	cost.C += inputRowCount * float64(aggsCount+groupingColCount) * cpuCostFactor

	// Add a cost that reflects the use of a hash table - unless we are doing a
	// streaming aggregation.
	//
	// The cost is chosen so that it's always less than the cost to sort the
	// input.
	if groupingColCount > 0 && streamingType != memo.Streaming {
		// Add the cost to build the hash table.
		cost.C += inputRowCount * cpuCostFactor

		// Add a cost for buffering rows that takes into account increased memory
		// pressure and the possibility of spilling to disk.
		cost.Add(c.rowBufferCost(outputRowCount))
	}

	return cost
}

func (c *coster) computeLimitCost(limit *memo.LimitExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost{C: limit.Relational().Statistics().RowCount * cpuCostFactor}
	return cost
}

func (c *coster) computeOffsetCost(offset *memo.OffsetExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost{C: offset.Relational().Statistics().RowCount * cpuCostFactor}
	return cost
}

func (c *coster) computeOrdinalityCost(ord *memo.OrdinalityExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost{C: ord.Relational().Statistics().RowCount * cpuCostFactor}
	return cost
}

func (c *coster) computeProjectSetCost(projectSet *memo.ProjectSetExpr) memo.Cost {
	// Add the CPU cost of emitting the rows.
	cost := memo.Cost{C: projectSet.Relational().Statistics().RowCount * cpuCostFactor}
	return cost
}

// getOrderingColStats returns the column statistic for the columns in the
// OrderingChoice oc. The OrderingChoice should be a member of expr. We include
// the Memo as an argument so that functions that call this function can be used
// both inside and outside the coster.
func getOrderingColStats(
	mem *memo.Memo, expr memo.RelExpr, oc props.OrderingChoice,
) *props.ColumnStatistic {
	if oc.Any() {
		return nil
	}
	stats := expr.Relational().Statistics()
	orderedCols := oc.ColSet()
	orderedStats, ok := stats.ColStats.Lookup(orderedCols)
	if !ok {
		orderedStats, ok = mem.RequestColStat(expr, orderedCols)
		if !ok {
			// I don't think we can ever get here. Since we don't allow the memo
			// to be optimized twice, the coster should never be used after
			// logPropsBuilder.clear() is called.
			panic(errors.AssertionFailedf("could not request the stats for ColSet %v", orderedCols))
		}
	}
	return orderedStats
}

// countSegments calculates the number of segments that will be used to execute
// the sort. If no input ordering is provided, there's only one segment.
func (c *coster) countSegments(sort *memo.SortExpr) float64 {
	orderedStats := getOrderingColStats(c.mem, sort, sort.InputOrdering)
	if orderedStats == nil {
		return 1
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
	return memo.Cost{C: cost}
}

// rowScanCost is the CPU cost to scan one row, which depends on the average
// size of the columns in the index and the average size of the columns we are
// scanning.
func (c *coster) rowScanCost(tabID opt.TableID, idxOrd int, scannedCols opt.ColSet) memo.Cost {
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
	var costFactor float64 = cpuCostFactor
	if !tab.IsVirtualTable() && len(c.locality.Tiers) != 0 {
		// If 0% of locality tiers have matching constraints, then add additional
		// cost. If 100% of locality tiers have matching constraints, then add no
		// additional cost. Anything in between is proportional to the number of
		// matches.
		adjustment := 1.0 - localityMatchScore(idx.Zone(), c.locality)
		costFactor += latencyCostFactor * adjustment
	}

	// The number of the columns in the index matter because more columns means
	// more data to scan. The number of columns we actually return also matters
	// because that is the amount of data that we could potentially transfer over
	// the network.
	if c.evalCtx != nil && c.evalCtx.SessionData().CostScansWithDefaultColSize {
		numScannedCols := scannedCols.Len()
		return memo.Cost{C: float64(numCols+numScannedCols) * costFactor}
	}
	var cost float64
	for i := 0; i < idx.ColumnCount(); i++ {
		colID := tabID.ColumnID(idx.Column(i).Ordinal())
		isScannedCol := scannedCols.Contains(colID)
		isSystemCol := idx.Column(i).Kind() == cat.System
		if isSystemCol && !isScannedCol {
			continue
		}
		avgSize := c.mem.RequestColAvgSize(tabID, colID)
		// Scanned columns are double-counted due to the cost of transferring data
		// over the network.
		var networkCostFactor float64 = 1
		if isScannedCol && !isSystemCol {
			networkCostFactor = 2
		}
		// Divide the column size by the default column size (4 bytes), so that by
		// default the cost of plans involving tables that use the default AvgSize
		// (e.g., if the stat is not available) is the same as if
		// CostScansWithDefaultColSize were true.
		cost += (float64(avgSize) / 4) * costFactor * networkCostFactor
	}
	return memo.Cost{C: cost}
}

// rowBufferCost adds a cost for buffering rows according to a ramp function:
//
//	               cost
//	              factor
//
//	                 |               spillRowCount
//	spillCostFactor _|                  ___________ _ _ _
//	                 |                 /
//	                 |                /
//	                 |               /
//	             0  _| _ _ _________/______________________    row
//	                 |                                        count
//	                      noSpillRowCount
//
// This function models the fact that operators that buffer rows become more
// expensive the more rows they need to buffer, since eventually they will need
// to spill to disk. The exact number of rows that cause spilling to disk varies
// depending on a number of factors that we don't model here. Therefore, we use
// a ramp function rather than a step function to account for the uncertainty
// and avoid sudden surprising plan changes due to a small change in stats.
func (c *coster) rowBufferCost(rowCount float64) memo.Cost {
	if rowCount <= noSpillRowCount {
		return memo.Cost{C: 0}
	}
	var fraction float64
	if rowCount >= spillRowCount {
		fraction = 1
	} else {
		fraction = (rowCount - noSpillRowCount) / (spillRowCount - noSpillRowCount)
	}

	return memo.Cost{C: rowCount * spillCostFactor * fraction}
}

// largeCardinalityCostPenalty returns a penalty that should be added to the
// cost of scans. It is non-zero for expressions with unbounded maximum
// cardinality or with maximum cardinality exceeding the row count estimate.
// Adding a few rows worth of cost helps prevent surprising plans for very small
// tables or for when stats are stale.
func (c *coster) largeCardinalityCostPenalty(
	cardinality props.Cardinality, rowCount float64,
) memo.Cost {
	if cardinality.IsUnbounded() {
		return memo.Cost{C: unboundedMaxCardinalityScanCostPenalty}
	}
	if maxCard := float64(cardinality.Max); maxCard > rowCount {
		penalty := maxCard - rowCount
		if penalty > largeMaxCardinalityScanCostPenalty {
			penalty = largeMaxCardinalityScanCostPenalty
		}
		return memo.Cost{C: penalty}
	}
	return memo.Cost{C: 0}
}

// localityMatchScore returns a number from 0.0 to 1.0 that describes how well
// the current node's locality matches the given zone constraints and
// leaseholder preferences, with 0.0 indicating 0% and 1.0 indicating 100%. This
// is the basic algorithm:
//
//	t = total # of locality tiers
//
//	Match each locality tier against the constraint set, and compute a value
//	for each tier:
//
//	   0 = key not present in constraint set or key matches prohibited
//	       constraint, but value doesn't match
//	  +1 = key matches required constraint, and value does match
//	  -1 = otherwise
//
//	m = length of longest locality prefix that ends in a +1 value and doesn't
//	    contain a -1 value.
//
//	Compute "m" for both the ReplicaConstraints constraints set, as well as for
//	the LeasePreferences constraints set:
//
//	  constraint-score = m / t
//	  lease-pref-score = m / t
//
//	if there are no lease preferences, then final-score = lease-pref-score
//	else final-score = (constraint-score * 2 + lease-pref-score) / 3
//
// Here are some scoring examples:
//
//	Locality = region=us,dc=east
//	0.0 = []                     // No constraints to match
//	0.0 = [+region=eu,+dc=uk]    // None of the tiers match
//	0.0 = [+region=eu,+dc=east]  // 2nd tier matches, but 1st tier doesn't
//	0.0 = [-region=us,+dc=east]  // 1st tier matches PROHIBITED constraint
//	0.0 = [-region=eu]           // 1st tier PROHIBITED and non-matching
//	0.5 = [+region=us]           // 1st tier matches
//	0.5 = [+region=us,-dc=east]  // 1st tier matches, 2nd tier PROHIBITED
//	0.5 = [+region=us,+dc=west]  // 1st tier matches, but 2nd tier doesn't
//	1.0 = [+region=us,+dc=east]  // Both tiers match
//	1.0 = [+dc=east]             // 2nd tier matches, no constraints for 1st
//	1.0 = [+region=us,+dc=east,+rack=1,-ssd]  // Extra constraints ignored
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

// streamingGroupByLimitHint calculates an appropriate limit hint for the input
// to a streaming GroupBy expression.
func streamingGroupByInputLimitHint(
	inputRowCount, outputRowCount, outputLimitHint float64,
) float64 {
	if outputRowCount == 0 {
		return 0
	}

	// Estimate the number of input rows needed to output LimitHint rows.
	inputLimitHint := outputLimitHint * inputRowCount / outputRowCount
	return math.Min(inputRowCount, inputLimitHint)
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

// topKInputLimitHint calculates an appropriate limit hint for the input
// to a Top K expression when the input is partially sorted.
func topKInputLimitHint(
	mem *memo.Memo, topk *memo.TopKExpr, inputRowCount, outputRowCount, K float64,
) float64 {
	if outputRowCount == 0 {
		return 0
	}
	orderedStats := getOrderingColStats(mem, topk, topk.PartialOrdering)
	if orderedStats == nil {
		return inputRowCount
	}

	// In order to find the top K rows of a partially sorted input, we estimate
	// the number of rows we'll need to ingest by rounding up the nearest multiple
	// of the number of rows per distinct values to K. For example, let's say we
	// have 2000 input rows, 100 distinct values, and a K of 10. If we assume that
	// each distinct value is found in the same number of input rows, each
	// distinct value has 2000/100 = 20 rowsPerDistinctVal. Processing the rows
	// for one distinct value is sufficient to find the top K 10 rows. If K were
	// 50 instead, we would need to process more distinct values to find the top
	// K, so we need to multiply the rowsPerDistinctVal by the minimum number of
	// distinct values to process, which we can find by dividing K by the rows per
	// distinct values and rounding up, or ceil(50/20) = 3. So if K is 50, we need
	// to process approximately 3 * 20 = 60 rows to find the top 50 rows.
	rowsPerDistinctVal := inputRowCount / orderedStats.DistinctCount
	expectedRows := math.Ceil(K/rowsPerDistinctVal) * rowsPerDistinctVal
	return math.Min(inputRowCount, expectedRows)
}

// indexLookupJoinPerLookupCost accounts for the cost of performing lookups for
// a single input row. It accounts for the random IOs incurred for each span
// (multiple spans mean multiple IOs). It also accounts for the extra CPU cost
// of the lookupExpr, if there is one.
func indexLookupJoinPerLookupCost(join memo.RelExpr) memo.Cost {
	// The rows in the (left) input are used to probe into the (right) table.
	// Since the matching rows in the table may not all be in the same range,
	// this counts as random I/O.
	cost := memo.Cost{C: randIOCostFactor}
	lookupJoin, ok := join.(*memo.LookupJoinExpr)
	if ok && len(lookupJoin.LookupExpr) > 0 {
		numSpans := 1
		var getNumSpans func(opt.ScalarExpr)
		getNumSpans = func(expr opt.ScalarExpr) {
			// The lookup expression will have been validated by isCanonicalFilter in
			// lookupjoin/constraint_builder.go to only contain a subset of possible
			// filter condition types.
			switch t := expr.(type) {
			case *memo.RangeExpr:
				getNumSpans(t.And)
			case *memo.AndExpr:
				getNumSpans(t.Left)
				getNumSpans(t.Right)
			case *memo.InExpr:
				in := t.Right.(*memo.TupleExpr)
				numSpans *= len(in.Elems)
			default:
				// Equalities and inequalities do not change the number of spans.
			}
		}
		if numSpans == 0 {
			panic(errors.AssertionFailedf("lookup expr has contradiction"))
		}
		for i := range lookupJoin.LookupExpr {
			getNumSpans(lookupJoin.LookupExpr[i].Condition)
		}
		if numSpans > 1 {
			// Account for the random IO incurred by looking up the extra spans.
			cost.C += randIOCostFactor * float64(numSpans-1)
		}
		// 1.1 is a fudge factor that pushes some plans over the edge when choosing
		// between a partial index vs full index plus lookup expr in the
		// regional_by_row.
		// TODO(treilly): do some empirical analysis and model this better
		cost.C += cpuCostFactor * float64(len(lookupJoin.LookupExpr)) * 1.1
	}
	return cost
}
