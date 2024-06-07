// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import map from "lodash/map";
import { AxisUnits, util } from "@cockroachlabs/cluster-ui";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import { cockroach } from "src/js/protos";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

import TimeSeriesQueryAggregator = cockroach.ts.tspb.TimeSeriesQueryAggregator;

export default function (props: GraphDashboardProps) {
  const { storeSources, tenantSource, nodeIDs,nodeDisplayNameByID} = props;

  return [
    <LineGraph
      title="Logical Bytes Received"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Rate at which the logical bytes (sum of keys + values) are received by all logical replication jobs`}
    >
      <Axis units={AxisUnits.Bytes} label="bytes">
        <Metric
          name="cr.node.logical_replication.logical_bytes"
          title="Logical Bytes Received"
          nonNegativeRate
        />
      </Axis>
    </LineGraph>,

     <LineGraph
     title="SQL Events Applied"
     sources={storeSources}
     tenantSource={tenantSource}
     tooltip={`Rate at which INSERT, UPDATE, and DELETE statements are applied by all logical replication jobs`}
   >
     <Axis label="events">
       <Metric
         name="cr.node.logical_replication.events_ingested"
         title="SQL Events Applied"
         nonNegativeRate
       />
     </Axis>
   </LineGraph>,

    <LineGraph
    title="Event Commit Latency: 50th percentile"
    isKvGraph={false}
    tenantSource={tenantSource}
    tooltip={"The 50th percentile in the difference in INSERT, UPDATE, and DELETE commit times between the source cluster and the destination cluster"}
    showMetricsInTooltip={true}
  >
    <Axis units={AxisUnits.Duration} label="latency">
      {map(nodeIDs, node => (
        <Metric
          key={node}
          name="cr.node.logical_replication.commit_latency-50"
          title={nodeDisplayName(nodeDisplayNameByID, node)}
          sources={[node]}
          downsampleMax
        />
      ))}
    </Axis>
  </LineGraph>,

   <LineGraph
   title="Event Commit Latency: 95th percentile"
   isKvGraph={false}
   tenantSource={tenantSource}
   tooltip={"The 95th percentile in the difference in INSERT, UPDATE, and DELETE commit times between the source cluster and the destination cluster"}
   showMetricsInTooltip={true}
 >
   <Axis units={AxisUnits.Duration} label="latency">
     {map(nodeIDs, node => (
       <Metric
         key={node}
         name="cr.node.logical_replication.commit_latency-95"
         title={nodeDisplayName(nodeDisplayNameByID, node)}
         sources={[node]}
         downsampleMax
       />
     ))}
   </Axis>
 </LineGraph>,

    <LineGraph
      title="Age of Oldest Row Pending Replication"
      sources={storeSources}
      tenantSource={tenantSource}
      tooltip={`Age of Oldest Row on Source that has yet to replicate to destination cluster`}
    >
      <Axis units={AxisUnits.Duration} label="duration">
        <Metric
          downsampler={TimeSeriesQueryAggregator.MIN}
          aggregator={TimeSeriesQueryAggregator.MAX}
          name="cr.node.logical_replication.replicated_time_seconds"
          title="Age of Oldest Row Pending Replication"
          transform={datapoints =>
            datapoints
              .filter(d => d.value !== 0)
              .map(d =>
                d.value
                  ? {
                      ...d,
                      value:
                        d.timestamp_nanos.toNumber() -
                        util.SecondsToNano(d.value),
                    }
                  : d,
              )
          }
        />
      </Axis>
    </LineGraph>,
  ];
}
