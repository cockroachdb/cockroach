// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { UpOutlined, DownOutlined } from "@ant-design/icons";
import { TimeScale } from "@cockroachlabs/cluster-ui/dist/types/timeScaleDropdown";
import { PayloadAction } from "@reduxjs/toolkit";
import React, { useState } from "react";
import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";

import {
  setMetricsFixedWindow,
  setTimeScale,
  TimeWindow,
} from "oss/src/redux/timeScale";
import { MetricsDataProvider } from "oss/src/views/shared/containers/metricDataProvider";
import { getCookieValue } from "src/redux/cookies";
import {
  metricOptionsSelector,
  MetricsMetadata,
  metricsMetadataSelector,
} from "src/redux/metricMetadata";
import {
  NodesSummary,
  nodeOptionsSelector,
  nodesSummarySelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { tenantDropdownOptions } from "src/redux/tenants";
import LineGraph from "src/views/cluster/components/linegraph";
import { DropdownOption } from "src/views/shared/components/dropdown";
import { Axis } from "src/views/shared/components/metricQuery";

import {
  CustomChartState,
  CustomChartTable,
} from "../../customChart/customMetric";
import { generateGraphTitleFromMetrics, GraphConfig } from "../dashboardConfig";

import CustomMetric from "./customMetric";
import EditableTitle from "./editableTitle";

export type GraphContainerOwnProps = {
  graphConfig: GraphConfig;
  onConfigChange: (index: number, newGraphConfig: GraphConfig) => void;
  index: number;
  onDelete: (index: number) => void;
} & RouteComponentProps;

export type GraphContainerProps = GraphContainerOwnProps & {
  nodesSummary: NodesSummary;
  tenantOptions: DropdownOption[];
  metricOptions: DropdownOption[];
  nodeOptions: DropdownOption[];
  currentTenant: string | null;
  metricsMetadata: MetricsMetadata;
  setMetricsFixedWindow: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;
};

const GraphContainer: React.FC<GraphContainerProps> = ({
  graphConfig,
  index,
  nodesSummary,
  tenantOptions,
  metricOptions,
  nodeOptions,
  currentTenant,
  history,
  metricsMetadata,
  onConfigChange,
  onDelete,
  setMetricsFixedWindow,
  setTimeScale,
}) => {
  const tenants = tenantOptions.slice(1).map(tenant => tenant.value);
  const [isTableCollapsed, setIsTableCollapsed] = useState(false);

  const onChange = (_index: number, newState: CustomChartState) => {
    onConfigChange(index, {
      ...graphConfig,
      metrics: newState.metrics,
      axis: {
        ...graphConfig.axis,
        units: newState.axisUnits,
      },
    });
  };

  const handleTitleChange = (newTitle: string) => {
    onConfigChange(index, {
      ...graphConfig,
      title: newTitle,
    });
  };

  // We require nodes information to determine sources (storeIDs/nodeIDs) down below.
  if (!(nodesSummary?.nodeStatuses?.length > 0)) {
    return;
  }

  return (
    <div
      className="metrics-workspace__graph-container"
      style={{ marginBottom: "32px" }}
    >
      <div
        style={{
          marginBottom: "12px",
          paddingLeft: "4px",
        }}
      >
        <EditableTitle
          value={graphConfig.title}
          onChange={handleTitleChange}
          placeholder={generateGraphTitleFromMetrics(graphConfig.metrics)}
          size="medium"
        />
      </div>
      {graphConfig.metrics.length > 0 && (
        <MetricsDataProvider
          id={`custom-dashboard-graph-${index}`}
          key={`custom-dashboard-graph-${index}-${graphConfig.axis.units}`}
          setMetricsFixedWindow={setMetricsFixedWindow}
          setTimeScale={setTimeScale}
          history={history}
        >
          <LineGraph title="">
            <Axis
              units={graphConfig.axis.units}
              label={graphConfig.axis.label}
            />
            {/* We have to call CustomMetric as a function instead of mounting it as a component
          LinGraph won't render its children. To query metrics, it does a DFS to find <Mteric> 
          componenets in props.children.
           */}
            {graphConfig.metrics.map((metric, index) =>
              CustomMetric({
                key: index,
                metric: metric,
                tenants: tenants,
                nodesSummary: nodesSummary,
                metricsMetadata: metricsMetadata,
              }),
            )}
          </LineGraph>
        </MetricsDataProvider>
      )}
      <div style={{ marginTop: "12px" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-start",
            marginBottom: "8px",
          }}
        >
          <span
            onClick={() => setIsTableCollapsed(!isTableCollapsed)}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: "4px",
              fontSize: "12px",
              color: "#8c8c8c",
              cursor: "pointer",
              padding: "4px 8px",
              borderRadius: "4px",
              transition: "background-color 0.2s",
            }}
            onMouseEnter={e => {
              e.currentTarget.style.backgroundColor = "rgba(0, 0, 0, 0.02)";
            }}
            onMouseLeave={e => {
              e.currentTarget.style.backgroundColor = "transparent";
            }}
          >
            {isTableCollapsed ? (
              <>
                <DownOutlined style={{ fontSize: "10px" }} />
                <span>Show metrics</span>
              </>
            ) : (
              <>
                <UpOutlined style={{ fontSize: "10px" }} />
                <span>Hide metrics</span>
              </>
            )}
          </span>
        </div>
        {!isTableCollapsed && (
          <div>
            <CustomChartTable
              metricOptions={metricOptions}
              nodeOptions={nodeOptions}
              tenantOptions={tenantOptions}
              currentTenant={currentTenant}
              index={index}
              chartState={{
                metrics: graphConfig.metrics,
                axisUnits: graphConfig.axis.units,
              }}
              onChange={onChange}
              onDelete={onDelete}
            />
          </div>
        )}
      </div>
    </div>
  );
};

const mapStateToProps = (state: AdminUIState) => ({
  nodesSummary: nodesSummarySelector(state),
  tenantOptions: tenantDropdownOptions(state),
  metricOptions: metricOptionsSelector(state),
  nodeOptions: nodeOptionsSelector(state),
  metricsMetadata: metricsMetadataSelector(state),
  currentTenant: getCookieValue("tenant"),
});

const mapDispatchToProps = {
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale: setTimeScale,
};

export default withRouter<
  GraphContainerOwnProps,
  React.ComponentType<GraphContainerOwnProps>
>(
  connect<{}, {}, GraphContainerOwnProps, AdminUIState>(
    mapStateToProps,
    mapDispatchToProps,
  )(GraphContainer),
);
