/// <reference path="../../typings/main.d.ts" />
import * as React from "react";
import * as d3 from "d3";
import _ = require("lodash");

import { MetricsDataProvider } from "../containers/metricsDataProvider";
import { LineGraph, Axis, Metric } from "../components/linegraph";
import { Bytes } from "../util/format";

let clusterMainGraphs = [
  <LineGraph title="SQL Connections"
             tooltip="The total number of active SQL connections to the cluster.">
    <Axis format={ d3.format(".1") }>
      <Metric name="cr.node.sql.conns" title="Connections" />
    </Axis>
  </LineGraph>,

  <LineGraph title="SQL Traffic"
             tooltip="The amount of network traffic sent to and from the SQL system, in bytes.">
    <Axis format={ Bytes }>
      <Metric name="cr.node.sql.bytesin" title="Bytes In" nonNegativeRate />
      <Metric name="cr.node.sql.inserts" title="Bytes Out" nonNegativeRate />
    </Axis>
  </LineGraph>,

  <LineGraph title="Reads Per Second"
             tooltip="The number of SELECT statements, averaged over a 10 second period.">
    <Axis format={ d3.format(".1") }>
      <Metric name="cr.node.sql.select.count" title="Selects" nonNegativeRate />
    </Axis>
  </LineGraph>,

  <LineGraph title="Writes Per Second"
             tooltip="The number of INSERT, UPDATE, and DELETE statements, averaged over a 10 second period.">
    <Axis format={ d3.format(".1") }>
      <Metric name="cr.node.sql.insert.count" title="Insert" nonNegativeRate />
      <Metric name="cr.node.sql.update.count" title="Update" nonNegativeRate />
      <Metric name="cr.node.sql.delete.count" title="Delete" nonNegativeRate />
    </Axis>
  </LineGraph>,
];

/**
 * ClusterMain renders the main content of the cluster page.
 */
export class ClusterMain extends React.Component<{}, {}> {
  render() {
    return <div className="section">
      <div className="charts">
      {
        // Render each chart inside of a MetricsDataProvider with a generated
        // key.
        _.map(clusterMainGraphs, (graph, idx) => {
          let key = "cluster." + idx.toString();
          return <div style={{float:"left"}} key={key}>
            <MetricsDataProvider id={key}>
              { graph }
            </MetricsDataProvider>
          </div>;
        })
      }
      </div>
    </div>;
  }
}

/**
 * ClusterTitle renders the header of the cluster page.
 */
export class ClusterTitle extends React.Component<{}, {}> {
  render() {
    return <h2>Cluster</h2>;
  }
}
