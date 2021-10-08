// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";
import * as d3 from "d3";

import { NodesSummary } from "src/redux/nodes";
import { Bytes } from "src/util/format";
import { NanoToMilli } from "src/util/convert";

import { EventBox } from "src/views/cluster/containers/events";
import { Metric } from "src/views/shared/components/metricQuery";
import {
  SummaryBar,
  SummaryLabel,
  SummaryMetricStat,
  SummaryStat,
  SummaryStatBreakdown,
  SummaryStatMessage,
  SummaryMetricsAggregator,
} from "src/views/shared/components/summaryBar";
import { Tooltip, Anchor } from "src/components";
import { howAreCapacityMetricsCalculated } from "src/util/docs";
import { AdminUIState } from "src/redux/state";

/**
 * ClusterNodeTotals displays a high-level breakdown of the nodes on the cluster
 * and their current liveness status.
 */

export interface ClusterNodeTotalsProps {
  nodesSummary: NodesSummary;
  nodesSummaryEmpty: boolean;
}

export const ClusterNodeTotalsComponent: React.FC<ClusterNodeTotalsProps> = ({
  nodesSummary,
  nodesSummaryEmpty,
}) => {
  if (nodesSummaryEmpty) {
    return null;
  }
  const { nodeCounts } = nodesSummary.nodeSums;
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

export function selectNodesSummaryEmpty(state: AdminUIState) {
  return !state.cachedData.nodes.data;
}

const mapStateToProps = (state: AdminUIState) => ({
  nodesSummaryEmpty: selectNodesSummaryEmpty(state),
});

const ClusterNodeTotals = connect(
  mapStateToProps,
  {},
)(ClusterNodeTotalsComponent);

const formatOnePlace = d3.format(".1f");
const formatPercentage = d3.format(".2%");
function formatNanosAsMillis(n: number) {
  return formatOnePlace(NanoToMilli(n)) + " ms";
}

/**
 * Component which displays the cluster summary bar on the graphs page.
 */
export interface ClusterSummaryProps {
  nodeSources: string[];
  nodesSummary: NodesSummary;
}

export default function(props: ClusterSummaryProps) {
  // Capacity math used in the summary status section.
  const { capacityUsed, capacityUsable } = props.nodesSummary.nodeSums;
  const capacityPercent =
    capacityUsable !== 0 ? capacityUsed / capacityUsable : null;
  return (
    <div>
      <SummaryBar>
        <SummaryLabel>Summary</SummaryLabel>
        <ClusterNodeTotals nodesSummary={props.nodesSummary} />
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
          value={props.nodesSummary.nodeSums.unavailableRanges}
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
            name="cr.node.sql.select.count"
            title="Queries/Sec"
            nonNegativeRate
          />
          <Metric
            sources={props.nodeSources}
            name="cr.node.sql.insert.count"
            title="Queries/Sec"
            nonNegativeRate
          />
          <Metric
            sources={props.nodeSources}
            name="cr.node.sql.update.count"
            title="Queries/Sec"
            nonNegativeRate
          />
          <Metric
            sources={props.nodeSources}
            name="cr.node.sql.delete.count"
            title="Queries/Sec"
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
