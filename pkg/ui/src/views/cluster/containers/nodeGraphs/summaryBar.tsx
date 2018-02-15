import React from "react";
import { Link } from "react-router";
import * as d3 from "d3";

import { NodesSummary } from "src/redux/nodes";
import { Bytes } from "src/util/format";
import { NanoToMilli } from "src/util/convert";

import { EventBox } from "src/views/cluster/containers/events";
import { Metric } from "src/views/shared/components/metricQuery";
import {
  SummaryBar, SummaryLabel, SummaryStat, SummaryStatMessage, SummaryStatBreakdown, SummaryMetricStat,
} from "src/views/shared/components/summaryBar";

interface ClusterSummaryProps {
  nodeSources: string[];
  nodesSummary: NodesSummary;
}

/**
 * ClusterNodeTotals displays a high-level breakdown of the nodes on the cluster
 * and their current liveness status.
 */
function ClusterNodeTotals (props: ClusterSummaryProps) {
  if (!props.nodesSummary || !props.nodesSummary.nodeSums) {
    return;
  }
  const { nodeCounts } = props.nodesSummary.nodeSums;
  let children: React.ReactNode;
  if (nodeCounts.dead > 0 || nodeCounts.suspect > 0) {
    children = (
      <div>
        <SummaryStatBreakdown title="Healthy" value={nodeCounts.healthy} modifier="healthy" />
        <SummaryStatBreakdown title="Suspect" value={nodeCounts.suspect} modifier="suspect" />
        <SummaryStatBreakdown title="Dead" value={nodeCounts.dead} modifier="dead" />
      </div>
    );
  }
  return (
    <SummaryStat
      title={
        <span>Total Nodes <Link to="/nodes">View nodes list</Link></span>
      }
      value={nodeCounts.total}
    >
      { children }
    </SummaryStat>
  );
}

const formatOnePlace = d3.format(".1f");
const formatPercentage = d3.format(".2%");
function formatNanosAsMillis (n: number) {
  return formatOnePlace(NanoToMilli(n)) + " ms";
}

/**
 * Component which displays the cluster summary bar on the graphs page.
 */
export default function(props: ClusterSummaryProps) {
  // Capacity math used in the summary status section.
  const { capacityUsed, capacityUsable } = props.nodesSummary.nodeSums;
  const capacityPercent = capacityUsable !== 0 ? (capacityUsed / capacityUsable) : null;

  return (
    <div>
      <SummaryBar>
        <SummaryLabel>Summary</SummaryLabel>
        <ClusterNodeTotals {...props}/>
        <SummaryStat title="Capacity Used" value={capacityPercent} format={formatPercentage}>
          <SummaryStatMessage message={`You are using ${Bytes(capacityUsed)} of ${Bytes(capacityUsable)}
                                        usable storage capacity across all nodes.`} />
        </SummaryStat>
        <SummaryStat title="Unavailable ranges" value={props.nodesSummary.nodeSums.unavailableRanges} />
        <SummaryMetricStat id="qps" title="Queries per second" format={formatOnePlace} >
          <Metric sources={props.nodeSources} name="cr.node.sql.query.count" title="Queries/Sec" nonNegativeRate />
        </SummaryMetricStat>
        <SummaryMetricStat id="p50" title="P50 latency" format={formatNanosAsMillis} >
          <Metric sources={props.nodeSources} name="cr.node.sql.service.latency-p50" aggregateMax downsampleMax />
        </SummaryMetricStat>
        <SummaryMetricStat id="p99" title="P99 latency" format={formatNanosAsMillis} >
          <Metric sources={props.nodeSources} name="cr.node.sql.service.latency-p99" aggregateMax downsampleMax />
        </SummaryMetricStat>
      </SummaryBar>
      <SummaryBar>
        <SummaryLabel>Events</SummaryLabel>
        <EventBox />
      </SummaryBar>
    </div>
  );
}
