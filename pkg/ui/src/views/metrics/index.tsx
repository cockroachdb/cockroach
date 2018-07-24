import React from "react";

import { LineGraph, LineGraphProps } from "src/views/cluster/components/linegraph";
import { nodeDisplayName, storeIDsForNode } from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";
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
  tooltipSelection: string;
  forwardProps: Partial<LineGraphProps>;
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
      // tslint:disable-next-line:variable-name
      const Tooltip = chart.tooltip;

      return (
        <MetricsDataProvider id={key}>
          <LineGraph
            sources={this.getSources(chart.sourceLevel)}
            title={chart.title}
            tooltip={<Tooltip selection={this.props.tooltipSelection} />}
            {...this.props.forwardProps}
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
            {...this.props.forwardProps}
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
