// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusterstats

/*
Package clusterstats provides abstractions to query and parse metrics that are
exported by a cockroach cluster, its VMs, and running workloads. This is
instrumented via connecting a prometheus process on a node in a roachtest,
which scrapes the endpoints declared in the prometheus config.

The abstractions provided are:

(1) StatCollector: collect point-in-time stats and timeseries stats in a roachtest.
(2) StatStreamer:  stream point-in-time stats for processing within a roachtest.
(3) StatExporter:  export a group of stats collected over a run to roachperf.

(1) StatCollector is the basic component used in (2) and (3) to query stats. It
and provides a simple interface to collect stats and return them in a parseable
format, without pkg dependencies.

The two important methods of the exporter are CollectPoint and CollectInterval.

CollectPoint requires a string, which is a query in the PromQL format, and a
time. It returns a map of label names to labeled stat values at the given time
(map[string]map[string]StatPoint).

e.g. If the query were rebalancing_queriespersecond at time 110 and there were
two stores (1,2) with ip addresses 10.0.0.1 and 127.0.0.1, for store 2 and
store 1 respectively.
{
   "store": {
	   "1": {Time: 100, Value: 777},
	   "2": {Time: 100, Value: 42},
   },
   "instance": {
		"10.0.0.1":  {Time: 100, Value: 42},
		"127.0.0.1": {Time: 100, Value: 777},
	},
}


CollectInterval requires a string, which is a query in the PromQL format, and
an time interval. It returns a map of label names to labeled timeseries values
that are contained in the time interval given
(map[string]map[string][]StatPoint).

e.g. If the query were rebalancing_queriespersecond in the interval [100,110]
and there were two stores (1,2) with ip addresses 10.0.0.1 and 127.0.0.1, for
store 2 and store 1 respectively.
{
   "store": {
       "1": {
           {Time: 100, Value: 777},
           {Time: 110, Value: 888}
       },
       "2": {
           {Time: 100, Value: 42},
           {Time: 110, Value 42},
       },
   },
   "instance": {
        "10.0.0.1":  {
           {Time: 100, Value: 42},
           {Time: 110, Value 42},
       },
        "127.0.0.1": {
           {Time: 100, Value: 777},
           {Time: 110, Value: 888}
       },
    },
}

(2) StatStreamer uses the StatCollector (1) in order to process registered
queries and return the parsed result, at a defined interval. The caller passes
in a function, ProcessStatEventFn, which the caller may use to process the
streamed values.

The StatStreamer does not export or persist the stats it collects and hands
back to ProcessStatEventFn. Instead, its primary purpose is as a utility for
triggering stages of a test. For example, a test that wishes to monitor the
"balance" of load across the cluster, triggering a new workload or node to be
included upon reaching a steady state in terms of store load.

The two important methods are Register and Run.

Register takes a list of queries, with an associated label name and includes
them in the set of stats to be streamed each interval.

e.g.

StatStreamer.Register(
	ClusterStat{Query: "rebalancing_queriespersecond", LabelName: "instance"},
	ClusterStat{Query: "rebalancing_writespersecond", LabelName: "store"},
)

Run takes a start time ctx, and logger. It will begin a loop that runs every
interval (defined as the default 10s or overridden by SetInterval). In each
iteration, the currently registered queries are executed against the
StatCollector. The results are combined and given to ProcessStatEventFn. The
loop continues indefinitely until either the context is cancelled or
ProcessStatEventFn returns true, indicating that it is done processing this
stream.

(3) StatExporter uses the StatCollector (1) in order to collect stats requested
for export to roachperf. The stats that are requested follow a hierarchical
2-layer format and are defined as an AggQuery.

The 2-layers of an AggQuery are (i) the PromQL query and label name that are
used to collect multiple time series at a high level of granularity.

e.g. The rebalancing_queriespersecond of each store. Would be defined as:

AggQuery.Stat = ClusterStat{Query: "rebalancing_queriespersecond", LabelName: "store"}

(ii) The aggregation of (i), which may be either another PromQL query or user
defined aggregaton function. This aggregates across the multiple time series of
(i), into a single timeseries result.

e.g. The sum of QPS across all stores. Would be defined as:

AggQuery.Query = "sum(rebalancing_queriespersecond)"

Alternatively, a user defined function may be used, which takes in a matrix of
values, where a row contains all the values recorded with the same time. The
columns are ordered by label value.

e.g. For the same example above, where we declared ClusterStat.LabelName "store":

"store": {
       "1": {
           {Time: 100, Value: 777},
           {Time: 110, Value: 888}
       },
       "2": {
           {Time: 100, Value: 42},
           {Time: 110, Value 42},
       },
}

Would translate into a matrix:

 1    2   <- store
[777, 42] <- (Time 100)
[888, 42] <- (Time 110)

AggQuery.AggFn = func(query string, matSeries [][]float64) (string, []float64) {
	agg := make([]float64, 0, 1)
	for _, series := range matSeries {
		curSum := 0.0
		for _, val := range series {
			curSum += val
		}
		agg = append(agg, curSum)
	}
	return fmt.Sprintf("sum(%s)", query), agg
}

would return "sum(rebalancing_queriespersecond)", [819, 930].

This is useful for functions which cannot be expressed in PromQL, such as
coefficient of variation.

The StatExporter will collect, parse and then serialize the list of AggQuerys
provided. The serialization is specific to the roachperf format. The serialized
export is written to the perf artifacts directory.
*/
