/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

import { MetricsDataProvider } from "../containers/metricsDataProvider";
import { TextGraph, Axis, Selector } from "../components/graphs";

/**
 * ClusterTitle renders the main content of the cluster page.
 */
export class ClusterMain extends React.Component<{}, {}> {
  render() {
    return <div className="section">
      <MetricsDataProvider id="graphone">
        <TextGraph>
          <Axis>
            <Selector name="cr.node.sql.conns" title="SqlConnections" />
            <Selector name="cr.node.sql.inserts" />
          </Axis>
        </TextGraph>
      </MetricsDataProvider>
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
