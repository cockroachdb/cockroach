import React from "react";
import _ from "lodash";

import { AggregationLevel } from "src/redux/aggregationLevel";
import * as docsURL from "src/util/docs";
import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName, storeIDsForNode } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodesSummary, nodeSources, storeSources, tooltipSelection } = props;

  const charts = [];

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Statements"
        sources={nodeSources}
        tooltip={
          `A ten-second moving average of the # of SELECT, INSERT, UPDATE, and DELETE operations
          started per second ${tooltipSelection}.`
        }
      >
        <Axis label="queries">
          <Metric name="cr.node.sql.select.count" title="Reads" nonNegativeRate />
          <Metric name="cr.node.sql.update.count" title="Updates" nonNegativeRate />
          <Metric name="cr.node.sql.insert.count" title="Inserts" nonNegativeRate />
          <Metric name="cr.node.sql.delete.count" title="Deletes" nonNegativeRate />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Statements"
        subtitle="Reads"
        tooltip={"......."}
      >
        <Axis label="queries">
          {
            _.map(nodeIDs, (node) => (
              <Metric
                key={node}
                name="cr.node.sql.select.count"
                title={nodeDisplayName(nodesSummary, node)}
                sources={[node]}
                nonNegativeRate
              />
            ))
          }
        </Axis>
      </LineGraph>,
    );
    charts.push(
      <LineGraph
        title="Statements"
        subtitle="Updates"
        tooltip={"......."}
      >
        <Axis label="queries">
          {
            _.map(nodeIDs, (node) => (
              <Metric
                key={node}
                name="cr.node.sql.update.count"
                title={nodeDisplayName(nodesSummary, node)}
                sources={[node]}
                nonNegativeRate
              />
            ))
          }
        </Axis>
      </LineGraph>,
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Service Latency"
        sources={nodeSources}
        tooltip={"....."}
      >
        <Axis units={AxisUnits.Duration} label="latency">
          <Metric name="cr.node.sql.service.latency-p99" title="Service Latency" downsampleMax aggregateAvg />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Service Latency"
        tooltip={(
          <div>
            Over the last minute, this node executed 99% of queries within this time.&nbsp;
              <em>This time does not include network latency between the node and client.</em>
          </div>
        )}
      >
        <Axis units={AxisUnits.Duration} label="latency">
          {
            _.map(nodeIDs, (node) => (
              <Metric
                key={node}
                name="cr.node.sql.service.latency-p99"
                title={nodeDisplayName(nodesSummary, node)}
                sources={[node]}
                downsampleMax
              />
            ))
          }
        </Axis>
      </LineGraph>,
    );
  }

  if (props.aggregationLevel === AggregationLevel.Cluster) {
    charts.push(
      <LineGraph
        title="Replicas"
        tooltip={"...."}
        sources={[].concat.apply([], nodeIDs.map((nid) => storeIDsForNode(nodesSummary, nid)))}
      >
        <Axis label="replicas">
          <Metric name="cr.store.replicas" title="Replicas" />
        </Axis>
      </LineGraph>,
    );
  } else {
    charts.push(
      <LineGraph
        title="Replicas"
        tooltip={(
          <div>
            The number of range replicas stored on this node.
            {" "}
            <em>Ranges are subsets of your data, which are replicated to ensure survivability.</em>
          </div>
        )}
      >
        <Axis label="replicas">
          {
            _.map(nodeIDs, (nid) => (
              <Metric
                key={nid}
                name="cr.store.replicas"
                title={nodeDisplayName(nodesSummary, nid)}
                sources={storeIDsForNode(nodesSummary, nid)}
              />
            ))
          }
        </Axis>
      </LineGraph>,
    );
  }

  charts.push(
    <LineGraph
      title="Capacity"
      sources={storeSources}
      tooltip={(
        <div>
          <dl>
            <dt>Capacity</dt>
            <dd>
              Total disk space available {tooltipSelection} to CockroachDB.
              {" "}
              <em>
                Control this value per node with the
                {" "}
                <code>
                  <a href={docsURL.startFlags} target="_blank">
                    --store
                  </a>
                </code>
                {" "}
                flag.
              </em>
            </dd>
            <dt>Available</dt>
            <dd>Free disk space available {tooltipSelection} to CockroachDB.</dd>
            <dt>Used</dt>
            <dd>Disk space used {tooltipSelection} by CockroachDB.</dd>
          </dl>
        </div>
      )}
    >
      <Axis units={AxisUnits.Bytes} label="capacity">
        <Metric name="cr.store.capacity" title="Capacity" />
        <Metric name="cr.store.capacity.available" title="Available" />
        <Metric name="cr.store.capacity.used" title="Used" />
      </Axis>
    </LineGraph>,
  );

  return charts;
}
