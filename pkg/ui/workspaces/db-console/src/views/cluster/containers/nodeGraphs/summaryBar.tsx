// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import * as d3 from "d3";
import React from "react";
import { useSelector } from "react-redux";
import { Link } from "react-router-dom";
import { createSelector } from "reselect";

import { Anchor, Tooltip } from "src/components";
import { nodeStatusesSelector, nodeSumsSelector } from "src/redux/nodes";
import { howAreCapacityMetricsCalculated } from "src/util/docs";
import { EventBox } from "src/views/cluster/containers/events";
import { Metric } from "src/views/shared/components/metricQuery";
import {
  SummaryBar,
  SummaryLabel,
  SummaryMetricsAggregator,
  SummaryMetricStat,
  SummaryStat,
  SummaryStatBreakdown,
  SummaryStatMessage,
} from "src/views/shared/components/summaryBar";

/**
 * ClusterNodeTotals displays a high-level breakdown of the nodes on the cluster
 * and their current liveness status.
 */
export const ClusterNodeTotals: React.FC = () => {
  const nodeSums = useSelector(nodeSumsSelector);
  const nodesSummaryEmpty = useSelector(selectNodesSummaryEmpty);
  if (nodesSummaryEmpty) {
    return null;
  }
  const { nodeCounts } = nodeSums;
  let children: React.ReactNode;
  if (nodeCounts.dead > 0 || nodeCounts.suspect > 0) {
    children = (
      <div>
        <SummaryStatBreakdown
          title="Healthy"
          value={nodeCounts.healthy}
          modifier="healthy"
        />
        <SummaryStatBreakdown
          title="Suspect"
          value={nodeCounts.suspect}
          modifier="suspect"
        />
        <SummaryStatBreakdown
          title="Dead"
          value={nodeCounts.dead}
          modifier="dead"
        />
      </div>
    );
  }
  return (
    <SummaryStat
      title={
        <span>
          Total Nodes <Link to="/overview/list">View nodes list</Link>
        </span>
      }
      value={nodeCounts.total}
    >
      {children}
    </SummaryStat>
  );
};

export const selectNodesSummaryEmpty = createSelector(
  nodeStatusesSelector,
  nodes => !nodes,
);

const formatOnePlace = d3.format(".1f");
const formatPercentage = d3.format(".2%");
function formatNanosAsMillis(n: number) {
  return formatOnePlace(util.NanoToMilli(n)) + " ms";
}

/**
 * Component which displays the cluster summary bar on the graphs page.
 */
export interface ClusterSummaryProps {
  nodeSources: string[];
  tenantSource?: string;
}

export default function (props: ClusterSummaryProps) {
  const { Bytes } = util;
  const nodeSums = useSelector(nodeSumsSelector);
  // Capacity math used in the summary status section.
  const { capacityUsed, capacityUsable } = nodeSums;
  const capacityPercent =
    capacityUsable !== 0 ? capacityUsed / capacityUsable : null;
  return (
    <div>
      <SummaryBar>
        <SummaryLabel>Summary</SummaryLabel>
        <ClusterNodeTotals />
        <SummaryStat
          title={
            <Tooltip
              placement="left"
              title={
                <>
                  <p>
                    Percentage of total usable disk space in use by CockroachDB
                    data.
                  </p>
                  <Anchor href={howAreCapacityMetricsCalculated}>
                    How is this metric calculated?
                  </Anchor>
                </>
              }
            >
              Capacity Usage
            </Tooltip>
          }
          value={capacityPercent}
          format={formatPercentage}
        >
          <SummaryStatMessage
            message={`You are using ${Bytes(capacityUsed)} of ${Bytes(
              capacityUsable,
            )} usable disk capacity across all nodes.`}
          />
        </SummaryStat>
        <SummaryStat
          title="Unavailable ranges"
          value={nodeSums.unavailableRanges}
          numberAlert={nodeSums.unavailableRanges > 0}
        />
        <SummaryMetricStat
          id="qps"
          title="Queries per second"
          format={formatOnePlace}
          aggregator={SummaryMetricsAggregator.SUM}
          summaryStatMessage="Sum of Selects, Updates, Inserts, and Deletes across your entire cluster."
        >
          <Metric
            sources={props.nodeSources}
            name="cr.node.sql.crud_query.count"
            title="Queries/Sec"
            tenantSource={props.tenantSource}
            nonNegativeRate
          />
        </SummaryMetricStat>
        <SummaryMetricStat
          id="p99"
          title="P99 latency"
          format={formatNanosAsMillis}
        >
          <Metric
            sources={props.nodeSources}
            name="cr.node.sql.service.latency-p99"
            tenantSource={props.tenantSource}
            aggregateMax
            downsampleMax
          />
        </SummaryMetricStat>
      </SummaryBar>
      <SummaryBar>
        <SummaryLabel>Events</SummaryLabel>
        <EventBox />
      </SummaryBar>
    </div>
  );
}
