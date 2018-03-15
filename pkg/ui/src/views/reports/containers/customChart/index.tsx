import _ from "lodash";
import * as React from "react";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";
import { createSelector } from "reselect";

import { refreshNodes } from "src/redux/apiReducers";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { LineGraph } from "src/views/cluster/components/linegraph";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";
import { Metric, Axis, AxisUnits } from "src/views/shared/components/metricQuery";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";

import { CustomMetricState, CustomMetricRow } from "./customMetric";
import "./customChart.styl";

import { NodeStatus$Properties } from "../../../../util/proto";

const axisUnitsOptions: DropdownOption[] = [
  AxisUnits.Count,
  AxisUnits.Bytes,
  AxisUnits.Duration,
].map(au => ({ label: AxisUnits[au], value: au.toString() }));

export interface CustomChartProps {
  refreshNodes: typeof refreshNodes;
  nodesQueryValid: boolean;
  nodesSummary: NodesSummary;
}

interface UrlState {
  metrics: string;
  units: string;
}

class CustomChart extends React.Component<CustomChartProps & WithRouterProps> {
  // Selector which computes dropdown options based on the nodes available on
  // the cluster.
  private nodeOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (nodeStatuses, nodeDisplayNameByID): DropdownOption[] => {
      const base = [{value: "", label: "Cluster"}];
      return base.concat(_.chain(nodeStatuses)
        .map(ns => {
          return {
            value: ns.desc.node_id.toString(),
            label: nodeDisplayNameByID[ns.desc.node_id],
          };
        })
        .sortBy(value => _.startsWith(value.label, "[decommissioned]"))
        .value(),
      );
    },
  );

  // Selector which computes dropdown options based on the metrics which are
  // currently being stored on the cluster.
  private metricOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (nodeStatuses): DropdownOption[] => {
      if (_.isEmpty(nodeStatuses)) {
        return [];
      }

      return _.keys(nodeStatuses[0].metrics).map(k => {
        const fullMetricName =
          isStoreMetric(nodeStatuses[0], k)
          ? "cr.store." + k
          : "cr.node." + k;

        return {
          value: fullMetricName,
          label: k,
        };
      });
    },
  );

  static title() {
    return "Custom Chart";
  }

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: CustomChartProps & WithRouterProps) {
    this.refresh(props);
  }

  currentMetrics(): CustomMetricState[] {
    try {
      return JSON.parse(this.props.location.query.metrics);
    } catch (e) {
      return [];
    }
  }

  updateUrl(newState: Partial<UrlState>) {
    const pathname = this.props.location.pathname;
    this.props.router.push({
      pathname,
      query: _.assign({}, this.props.location.query, newState),
    });
  }

  updateUrlMetrics(newState: CustomMetricState[]) {
    const metrics = JSON.stringify(newState);
    this.updateUrl({
      metrics,
    });
  }

  updateMetricRow = (index: number, newState: CustomMetricState) => {
    const arr = this.currentMetrics().slice();
    arr[index] = newState;
    this.updateUrlMetrics(arr);
  }

  addMetric = () => {
    this.updateUrlMetrics([...this.currentMetrics(), new CustomMetricState()]);
  }

  removeMetric = (index: number) => {
    const metrics = this.currentMetrics();
    this.updateUrlMetrics(metrics.slice(0, index).concat(metrics.slice(index + 1)));
  }

  currentAxisUnits(): AxisUnits {
    return +this.props.location.query.units || AxisUnits.Count;
  }

  changeAxisUnits = (selected: DropdownOption) => {
    this.updateUrl({
      units: selected.value,
    });
  }

  // Render a chart of the currently selected metrics.
  renderChart() {
    const metrics = this.currentMetrics();
    const units = this.currentAxisUnits();
    const { nodesSummary } = this.props;
    if (_.isEmpty(metrics)) {
      return (
        <section className="section">
          <h3>Click "Add Metric" to add a metric to the custom chart.</h3>
        </section>
      );
    }

    return (
      <section className="section">
        <MetricsDataProvider id="debug-custom-chart">
          <LineGraph>
            <Axis units={units}>
              {
                metrics.map((m, i) => {
                  if (m.metric !== "") {
                    if (m.perNode) {
                      return _.map(nodesSummary.nodeIDs, (nodeID) => (
                        <Metric
                          key={"${i}${nodeID}"}
                          title={`${nodeID}: ${m.metric} (${i})`}
                          name={m.metric}
                          aggregator={m.aggregator}
                          downsampler={m.downsampler}
                          derivative={m.derivative}
                          sources={
                            isStoreMetric(nodesSummary.nodeStatuses[0], m.metric)
                            ? _.map(nodesSummary.storeIDsByNodeID[nodeID] || [], n => n.toString())
                            : [nodeID]
                          }
                        />
                      ));
                    } else {
                      return (
                        <Metric
                          key={i}
                          title={`${m.metric} (${i}) `}
                          name={m.metric}
                          aggregator={m.aggregator}
                          downsampler={m.downsampler}
                          derivative={m.derivative}
                          sources={m.source === "" ? [] : [m.source]}
                        />
                      );
                    }
                  }
                  return "";
                })
              }
            </Axis>
          </LineGraph>
        </MetricsDataProvider>
      </section>
    );
  }

  // Render a table containing all of the currently added metrics, with editing
  // inputs for each metric.
  renderMetricsTable() {
    const metrics = this.currentMetrics();
    let table: JSX.Element = null;

    if (!_.isEmpty(metrics)) {
      table = (
        <table className="metric-table">
          <thead>
            <tr>
              <td className="metric-table__header">Metric Name</td>
              <td className="metric-table__header">Downsampler</td>
              <td className="metric-table__header">Aggregator</td>
              <td className="metric-table__header">Rate</td>
              <td className="metric-table__header">Source</td>
              <td className="metric-table__header">Per Node</td>
              <td className="metric-table__header metric-table__header--no-title"></td>
            </tr>
          </thead>
          <tbody>
            { metrics.map((row, i) =>
              <CustomMetricRow
                key={i}
                metricOptions={this.metricOptions(this.props.nodesSummary)}
                nodeOptions={this.nodeOptions(this.props.nodesSummary)}
                index={i}
                rowState={row}
                onChange={this.updateMetricRow}
                onDelete={this.removeMetric}
              />,
            )}
          </tbody>
        </table>
      );
    }

    return (
      <section className="section">
        { table }
        <button className="metric-edit-button metric-edit-button--add" onClick={this.addMetric}>Add Metric</button>
      </section>
    );
  }

  render() {
    const units = this.currentAxisUnits();
    return (
      <div>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown
              title="Units"
              selected={units.toString()}
              options={axisUnitsOptions}
              onChange={this.changeAxisUnits}
            />
          </PageConfigItem>
        </PageConfig>
        { this.renderChart() }
        { this.renderMetricsTable() }
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    nodesSummary: nodesSummarySelector(state),
    nodesQueryValid: state.cachedData.nodes.valid,
  };
}

const mapDispatchToProps = {
  refreshNodes,
};

export default connect(mapStateToProps, mapDispatchToProps)(withRouter(CustomChart));

function isStoreMetric(nodeStatus: NodeStatus$Properties, metricName: string) {
  return _.has(nodeStatus.store_statuses[0].metrics, metricName);
}
