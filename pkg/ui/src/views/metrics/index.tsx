import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { nodeDisplayName, storeIDsForNode } from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";
import { AggregationLevel } from "src/redux/aggregationLevel";
import { NodesSummary } from "src/redux/nodes";

import { allDashboards } from "./dashboards";
import { Chart, Units, SourceLevel } from "./dashboards/interface";

interface DashboardsPageProps {
  dashboard: string;
  aggregationLevel: AggregationLevel;
  nodeSources: string[];
  nodesSummary: NodesSummary;
}

function mapUnits(units: Units): AxisUnits {
  switch (units) {
    case Units.Duration: return AxisUnits.Duration;
    case Units.Bytes: return AxisUnits.Bytes;
    default: return AxisUnits.Count;
  }
}

export class DashboardsPage extends React.Component<DashboardsPageProps> {
  getDashboard() {
    if (this.props.dashboard in allDashboards) {
      return allDashboards[this.props.dashboard];
    }

    return allDashboards.overview;
  }

  getStoreIds = (node: string) => {
    return storeIDsForNode(this.props.nodesSummary, node);
  }

  getSources(sourceLevel: SourceLevel, node?: string) {
    if (node) {
      switch (sourceLevel) {
        case SourceLevel.Store: return this.getStoreIds(node);
        default: return [node];
      }
    } else {
      switch (sourceLevel) {
        case SourceLevel.Store: return [].concat.apply([], this.props.nodeSources.map(this.getStoreIds));
        default: return this.props.nodeSources;
      }
    }
  }

  renderChart = (chart: Chart, idx: number) => {
    const key = `nodes.${this.props.dashboard}.${idx}.${this.props.aggregationLevel}`;
    if (this.props.aggregationLevel === AggregationLevel.Cluster) {
      return (
        <MetricsDataProvider id={key}>
          <LineGraph
            sources={this.getSources(chart.sourceLevel)}
            title={chart.title}
            tooltip={chart.tooltip}
          >
            <Axis label={chart.axis.label} units={mapUnits(chart.axis.units)}>
              {
                chart.metrics.map((metric) => (
                  <Metric {...metric} key={metric.name} />
                ))
              }
            </Axis>
          </LineGraph>
        </MetricsDataProvider>
      );
    } else {
      return chart.metrics.map((metric, index) => (
        <MetricsDataProvider id={key + "." + index}>
          <LineGraph
            title={chart.title}
            subtitle={metric.title}
            tooltip={metric.tooltip}
          >
            <Axis label={chart.axis.label} units={mapUnits(chart.axis.units)}>
              {
                this.props.nodeSources.map((node) => (
                  <Metric
                    {...metric}
                    key={node}
                    name={metric.name}
                    title={nodeDisplayName(this.props.nodesSummary, node)}
                    sources={this.getSources(chart.sourceLevel, node)}
                  />
                ))
              }
            </Axis>
          </LineGraph>
        </MetricsDataProvider>
      ));
    }
  }

  render() {
    const dashboard = this.getDashboard();

    return (
      <React.Fragment>
        { dashboard.charts.map(this.renderChart) }
      </React.Fragment>
    );
  }
}
