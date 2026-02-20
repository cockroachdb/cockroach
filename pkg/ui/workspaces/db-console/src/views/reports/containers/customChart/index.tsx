// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits, TimeScale } from "@cockroachlabs/cluster-ui";
import flatMap from "lodash/flatMap";
import flow from "lodash/flow";
import isEmpty from "lodash/isEmpty";
import keys from "lodash/keys";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import startsWith from "lodash/startsWith";
import React, { useEffect, useMemo } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import TimeScaleDropdown from "oss/src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import { PayloadAction } from "src/interfaces/action";
import {
  refreshMetricMetadata,
  refreshNodes,
  refreshTenantsList,
} from "src/redux/apiReducers";
import { getCookieValue } from "src/redux/cookies";
import {
  MetricsMetadata,
  metricsMetadataSelector,
} from "src/redux/metricMetadata";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { tenantDropdownOptions } from "src/redux/tenants";
import {
  TimeWindow,
  setMetricsFixedWindow,
  selectTimeScale,
  setTimeScale,
} from "src/redux/timeScale";
import { INodeStatus } from "src/util/proto";
import { queryByName } from "src/util/query";
import LineGraph from "src/views/cluster/components/linegraph";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";
import { DropdownOption } from "src/views/shared/components/dropdown";
import { Metric, Axis } from "src/views/shared/components/metricQuery";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

import {
  CustomChartState,
  CustomChartTable,
  CustomMetricState,
} from "./customMetric";
import "./customChart.scss";

export interface CustomChartProps {
  refreshNodes: typeof refreshNodes;
  nodesQueryValid: boolean;
  nodesSummary: NodesSummary;
  refreshMetricMetadata: typeof refreshMetricMetadata;
  refreshTenantsList: typeof refreshTenantsList;
  metricsMetadata: MetricsMetadata;
  setMetricsFixedWindow: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  timeScale: TimeScale;
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;
  tenantOptions: ReturnType<() => DropdownOption[]>;
  currentTenant: string | null;
}

interface UrlState {
  charts: string;
}

export const getSources = (
  nodesSummary: NodesSummary,
  metricState: CustomMetricState,
  metricsMetadata: MetricsMetadata,
): string[] => {
  if (!(nodesSummary?.nodeStatuses?.length > 0)) {
    return [];
  }
  // If we have no nodeSource, and we're not asking for perSource metrics,
  // then the user is asking for cluster-wide metrics. We can return an empty
  // source list.
  if (metricState.nodeSource === "" && !metricState.perSource) {
    return [];
  }
  if (isStoreMetric(metricsMetadata.recordedNames, metricState.metric)) {
    // If a specific node is selected, return the storeIDs associated with that node.
    // Otherwise, we're at the cluster level, so we grab each store ID.
    const storeIDs = metricState.nodeSource
      ? nodesSummary.storeIDsByNodeID[metricState.nodeSource]
      : Object.values(nodesSummary.storeIDsByNodeID).flatMap(s => s);
    // Sort store IDs to ensure consistent ordering
    return storeIDs.sort();
  } else {
    // If it's not a store metric, and a specific nodeSource is chosen, just return that.
    // Otherwise, return all known node IDs.
    return metricState.nodeSource
      ? [metricState.nodeSource]
      : nodesSummary.nodeIDs;
  }
};

export function CustomChart({
  refreshNodes: refreshNodesAction,
  nodesQueryValid,
  nodesSummary,
  refreshMetricMetadata: refreshMetricMetadataAction,
  refreshTenantsList: refreshTenantsListAction,
  metricsMetadata,
  setMetricsFixedWindow: setMetricsFixedWindowAction,
  timeScale,
  setTimeScale: setTimeScaleAction,
  tenantOptions,
  currentTenant,
  location,
  history,
}: CustomChartProps & RouteComponentProps): React.ReactElement {
  // Dropdown options computed from cluster node statuses.
  const nodeOptions = useMemo((): DropdownOption[] => {
    const base = [{ value: "", label: "Cluster" }];
    return base.concat(
      flow(
        (statuses: INodeStatus[]) =>
          map(statuses, ns => ({
            value: ns.desc.node_id.toString(),
            label: nodesSummary.nodeDisplayNameByID[ns.desc.node_id],
          })),
        values =>
          sortBy(values, value => startsWith(value.label, "[decommissioned]")),
      )(nodesSummary.nodeStatuses),
    );
  }, [nodesSummary.nodeStatuses, nodesSummary.nodeDisplayNameByID]);

  // Dropdown options computed from the metrics currently stored on the cluster.
  const metricOptions = useMemo((): DropdownOption[] => {
    if (isEmpty(metricsMetadata?.metadata)) {
      return [];
    }

    return keys(metricsMetadata.recordedNames).map(k => {
      const fullMetricName = metricsMetadata.recordedNames[k];
      return {
        value: fullMetricName,
        label: k,
        description: metricsMetadata.metadata[k]?.help,
      };
    });
  }, [metricsMetadata]);

  // Refresh nodes on every render if the query is stale.
  useEffect(() => {
    if (!nodesQueryValid) {
      refreshNodesAction();
    }
  });

  // Fetch metric metadata and tenants list once on mount.
  useEffect(() => {
    refreshMetricMetadataAction();
    refreshTenantsListAction();
  }, [refreshMetricMetadataAction, refreshTenantsListAction]);

  const currentCharts = (): CustomChartState[] => {
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
  };

  const updateUrl = (newState: Partial<UrlState>) => {
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
  };

  const updateUrlCharts = (newState: CustomChartState[]) => {
    const charts = JSON.stringify(newState);
    updateUrl({ charts });
  };

  const updateChartRow = (index: number, newState: CustomChartState) => {
    const arr = currentCharts().slice();
    arr[index] = newState;
    updateUrlCharts(arr);
  };

  const addChart = () => {
    updateUrlCharts([...currentCharts(), new CustomChartState()]);
  };

  const removeChart = (index: number) => {
    const charts = currentCharts();
    updateUrlCharts(charts.slice(0, index).concat(charts.slice(index + 1)));
  };

  // This function handles the logic related to creating Metric components
  // based on perNode and perTenant flags.
  const renderMetricComponents = (
    metrics: CustomMetricState[],
    index: number,
  ) => {
    // We require nodes information to determine sources (storeIDs/nodeIDs) down below.
    if (!(nodesSummary?.nodeStatuses?.length > 0)) {
      return;
    }
    const tenants = tenantOptions.length > 1 ? tenantOptions.slice(1) : [];
    return metrics.map((m, i) => {
      if (m.metric === "") {
        return "";
      }
      const sources = getSources(nodesSummary, m, metricsMetadata);
      if (m.perSource && m.perTenant) {
        return flatMap(sources, source => {
          return tenants.map(tenant => (
            <Metric
              key={`${index}${i}${source}${tenant.value}`}
              title={`${source}-${tenant.value}: ${m.metric} (${i})`}
              name={m.metric}
              aggregator={m.aggregator}
              downsampler={m.downsampler}
              derivative={m.derivative}
              sources={[source]}
              tenantSource={tenant.value}
            />
          ));
        });
      } else if (m.perSource) {
        return map(sources, source => (
          <Metric
            key={`${index}${i}${source}`}
            title={`${source}: ${m.metric} (${i})`}
            name={m.metric}
            aggregator={m.aggregator}
            downsampler={m.downsampler}
            derivative={m.derivative}
            sources={[source]}
            tenantSource={m.tenantSource}
          />
        ));
      } else if (m.perTenant) {
        return tenants.map(tenant => (
          <Metric
            key={`${index}${i}${tenant.value}`}
            title={`${tenant.value}: ${m.metric} (${i})`}
            name={m.metric}
            aggregator={m.aggregator}
            downsampler={m.downsampler}
            derivative={m.derivative}
            sources={sources}
            tenantSource={tenant.value}
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
            sources={sources}
            tenantSource={m.tenantSource}
          />
        );
      }
    });
  };

  // Render a chart of the currently selected metrics.
  const renderChart = (chart: CustomChartState, index: number) => {
    const metrics = chart.metrics;
    const units = chart.axisUnits;
    return (
      <MetricsDataProvider
        id={`debug-custom-chart.${index}`}
        key={`${index}-${units}`}
        setMetricsFixedWindow={setMetricsFixedWindowAction}
        setTimeScale={setTimeScaleAction}
        history={history}
      >
        <LineGraph title={metrics.map(m => m.metric).join(", ")}>
          <Axis units={units} label={AxisUnits[units]}>
            {renderMetricComponents(metrics, index)}
          </Axis>
        </LineGraph>
      </MetricsDataProvider>
    );
  };

  const renderCharts = () => {
    const charts = currentCharts();

    if (isEmpty(charts)) {
      return <h3>Click "Add Chart" to add a chart to the custom dashboard.</h3>;
    }

    return charts.map(renderChart);
  };

  // Render a table containing all of the currently added metrics, with editing
  // inputs for each metric.
  const renderChartTables = () => {
    const charts = currentCharts();

    return (
      <>
        {charts.map((chart, i) => (
          <CustomChartTable
            metricOptions={metricOptions}
            nodeOptions={nodeOptions}
            tenantOptions={tenantOptions}
            currentTenant={currentTenant}
            index={i}
            key={i}
            chartState={chart}
            onChange={updateChartRow}
            onDelete={removeChart}
          />
        ))}
      </>
    );
  };

  // Note: the vertical spacing below is to ensure we can scroll the page up
  // enough for the drop-down metric menu to be visible.
  // TODO(radu): remove this when we upgrade to a better component.
  return (
    <>
      <Helmet title="Custom Chart | Debug" />
      <BackToAdvanceDebug history={history} />
      <section className="section">
        <h1 className="base-heading">Custom Chart</h1>
      </section>
      <PageConfig>
        <PageConfigItem>
          <TimeScaleDropdown
            currentScale={timeScale}
            setTimeScale={setTimeScaleAction}
          />
        </PageConfigItem>
        <button
          className="edit-button chart-edit-button chart-edit-button--add"
          onClick={addChart}
        >
          Add Chart
        </button>
      </PageConfig>
      <section className="section">
        <div className="l-columns">
          <div className="chart-group l-columns__left">{renderCharts()}</div>
          <div className="l-columns__right" />
        </div>
      </section>
      <section className="section">{renderChartTables()}</section>
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
      <br />
    </>
  );
}

const mapStateToProps = (state: AdminUIState) => ({
  nodesSummary: nodesSummarySelector(state),
  nodesQueryValid: state.cachedData.nodes.valid,
  metricsMetadata: metricsMetadataSelector(state),
  timeScale: selectTimeScale(state),
  tenantOptions: tenantDropdownOptions(state),
  currentTenant: getCookieValue("tenant"),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshMetricMetadata,
  refreshTenantsList,
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale: setTimeScale,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(CustomChart),
);

function isStoreMetric(
  recordedNames: Record<string, string>,
  metricName: string,
) {
  if (metricName?.startsWith("cr.store")) {
    return true;
  }
  return recordedNames[metricName]?.startsWith("cr.store") || false;
}
