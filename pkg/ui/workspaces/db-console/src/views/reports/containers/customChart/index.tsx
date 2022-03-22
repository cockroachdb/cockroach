// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import { refreshMetricMetadata, refreshNodes } from "src/redux/apiReducers";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { LineGraph } from "src/views/cluster/components/linegraph";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import { DropdownOption } from "src/views/shared/components/dropdown";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";
import {
  Metric,
  Axis,
  AxisUnits,
} from "src/views/shared/components/metricQuery";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import {
  MetricsMetadata,
  metricsMetadataSelector,
} from "src/redux/metricMetadata";
import { INodeStatus } from "src/util/proto";

import { CustomChartState, CustomChartTable } from "./customMetric";
import "./customChart.styl";
import { queryByName } from "src/util/query";
import { PayloadAction } from "src/interfaces/action";
import {
  TimeWindow,
  TimeScale,
  setMetricsFixedWindow,
  setTimeScale,
} from "src/redux/timeScale";

export interface CustomChartProps {
  refreshNodes: typeof refreshNodes;
  nodesQueryValid: boolean;
  nodesSummary: NodesSummary;
  refreshMetricMetadata: typeof refreshMetricMetadata;
  metricsMetadata: MetricsMetadata;
  setMetricsFixedWindow: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;
}

interface UrlState {
  charts: string;
}

export class CustomChart extends React.Component<
  CustomChartProps & RouteComponentProps
> {
  // Selector which computes dropdown options based on the nodes available on
  // the cluster.
  private nodeOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (nodeStatuses, nodeDisplayNameByID): DropdownOption[] => {
      const base = [{ value: "", label: "Cluster" }];
      return base.concat(
        _.chain(nodeStatuses)
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
    (_summary: NodesSummary, metricsMetadata: MetricsMetadata) =>
      metricsMetadata,
    (nodeStatuses, metadata = {}): DropdownOption[] => {
      if (_.isEmpty(nodeStatuses)) {
        return [];
      }

      return _.keys(nodeStatuses[0].metrics).map(k => {
        const fullMetricName = isStoreMetric(nodeStatuses[0], k)
          ? "cr.store." + k
          : "cr.node." + k;

        return {
          value: fullMetricName,
          label: k,
          description: metadata[k] && metadata[k].help,
        };
      });
    },
  );

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
  }

  componentDidMount() {
    this.refresh();
    this.props.refreshMetricMetadata();
  }

  componentDidUpdate() {
    this.refresh(this.props);
  }

  currentCharts(): CustomChartState[] {
    const { location } = this.props;
    const metrics = queryByName(location, "metrics");
    const charts = queryByName(location, "charts");

    if (metrics !== null) {
      try {
        return [
          {
            metrics: JSON.parse(metrics),
            axisUnits: AxisUnits.Count,
          },
        ];
      } catch (e) {
        return [new CustomChartState()];
      }
    }

    if (charts !== null) {
      try {
        return JSON.parse(charts);
      } catch (e) {
        return [new CustomChartState()];
      }
    }

    return [new CustomChartState()];
  }

  updateUrl(newState: Partial<UrlState>) {
    const { location, history } = this.props;
    const { pathname, search } = location;
    const urlParams = new URLSearchParams(search);

    Object.entries(newState).forEach(([key, value]) => {
      urlParams.set(key, value);
    });

    history.push({
      pathname,
      search: urlParams.toString(),
      state: newState,
    });
  }

  updateUrlCharts(newState: CustomChartState[]) {
    const charts = JSON.stringify(newState);
    this.updateUrl({
      charts,
    });
  }

  updateChartRow = (index: number, newState: CustomChartState) => {
    const arr = this.currentCharts().slice();
    arr[index] = newState;
    this.updateUrlCharts(arr);
  };

  addChart = () => {
    this.updateUrlCharts([...this.currentCharts(), new CustomChartState()]);
  };

  removeChart = (index: number) => {
    const charts = this.currentCharts();
    this.updateUrlCharts(
      charts.slice(0, index).concat(charts.slice(index + 1)),
    );
  };

  // Render a chart of the currently selected metrics.
  renderChart = (chart: CustomChartState, index: number) => {
    const metrics = chart.metrics;
    const units = chart.axisUnits;
    const { nodesSummary } = this.props;

    return (
      <MetricsDataProvider
        id={`debug-custom-chart.${index}`}
        key={index}
        setMetricsFixedWindow={this.props.setMetricsFixedWindow}
        setTimeScale={this.props.setTimeScale}
        history={this.props.history}
      >
        <LineGraph>
          <Axis units={units} label={AxisUnits[units]}>
            {metrics.map((m, i) => {
              if (m.metric !== "") {
                if (m.perNode) {
                  return _.map(nodesSummary.nodeIDs, nodeID => (
                    <Metric
                      key={`${index}${i}${nodeID}`}
                      title={`${nodeID}: ${m.metric} (${i})`}
                      name={m.metric}
                      aggregator={m.aggregator}
                      downsampler={m.downsampler}
                      derivative={m.derivative}
                      sources={
                        isStoreMetric(nodesSummary.nodeStatuses[0], m.metric)
                          ? _.map(
                              nodesSummary.storeIDsByNodeID[nodeID] || [],
                              n => n.toString(),
                            )
                          : [nodeID]
                      }
                    />
                  ));
                } else {
                  return (
                    <Metric
                      key={`${index}${i}`}
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
            })}
          </Axis>
        </LineGraph>
      </MetricsDataProvider>
    );
  };

  renderCharts() {
    const charts = this.currentCharts();

    if (_.isEmpty(charts)) {
      return <h3>Click "Add Chart" to add a chart to the custom dashboard.</h3>;
    }

    return charts.map(this.renderChart);
  }

  // Render a table containing all of the currently added metrics, with editing
  // inputs for each metric.
  renderChartTables() {
    const { nodesSummary, metricsMetadata } = this.props;
    const charts = this.currentCharts();

    return (
      <>
        {charts.map((chart, i) => (
          <CustomChartTable
            metricOptions={this.metricOptions(nodesSummary, metricsMetadata)}
            nodeOptions={this.nodeOptions(nodesSummary)}
            index={i}
            key={i}
            chartState={chart}
            onChange={this.updateChartRow}
            onDelete={this.removeChart}
          />
        ))}
      </>
    );
  }

  render() {
    return (
      <>
        <Helmet title="Custom Chart | Debug" />
        <section className="section">
          <h1 className="base-heading">Custom Chart</h1>
        </section>
        <PageConfig>
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
          <button
            className="edit-button chart-edit-button chart-edit-button--add"
            onClick={this.addChart}
          >
            Add Chart
          </button>
        </PageConfig>
        <section className="section">
          <div className="l-columns">
            <div className="chart-group l-columns__left">
              {this.renderCharts()}
            </div>
            <div className="l-columns__right"></div>
          </div>
        </section>
        <section className="section">{this.renderChartTables()}</section>
      </>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => ({
  nodesSummary: nodesSummarySelector(state),
  nodesQueryValid: state.cachedData.nodes.valid,
  metricsMetadata: metricsMetadataSelector(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshMetricMetadata,
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(CustomChart),
);

function isStoreMetric(nodeStatus: INodeStatus, metricName: string) {
  return _.has(nodeStatus.store_statuses[0].metrics, metricName);
}
