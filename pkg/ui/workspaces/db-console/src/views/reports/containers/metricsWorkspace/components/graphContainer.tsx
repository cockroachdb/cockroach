// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { UpOutlined, DownOutlined } from "@ant-design/icons";
import {
  useNodesSummary,
  useMetricMetadata,
  useTenants,
} from "@cockroachlabs/cluster-ui";
import isEmpty from "lodash/isEmpty";
import keys from "lodash/keys";
import React, { useMemo, useState } from "react";
import { useDispatch } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";

import { setMetricsFixedWindow, setTimeScale } from "oss/src/redux/timeScale";
import { MetricsDataProvider } from "oss/src/views/shared/containers/metricDataProvider";
import { getCookieValue } from "src/redux/cookies";
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

const GraphContainer: React.FC<GraphContainerOwnProps> = ({
  graphConfig,
  index,
  history,
  onConfigChange,
  onDelete,
}) => {
  const dispatch = useDispatch();
  const nodesSummary = useNodesSummary();
  const { data: metricsMetadata } = useMetricMetadata();
  const { tenants: tenantsList } = useTenants();
  const currentTenant = getCookieValue("tenant");

  const ALL_TENANTS_OPTION: DropdownOption = { label: "All", value: "" };
  const tenantOptions: DropdownOption[] = useMemo(() => {
    const options: DropdownOption[] = [ALL_TENANTS_OPTION];
    tenantsList?.forEach(tenant =>
      options.push({
        label: tenant.tenant_name,
        value: tenant.tenant_id?.id?.toString(),
      }),
    );
    return options;
  }, [tenantsList]);

  const metricOptions: DropdownOption[] = useMemo(() => {
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

  const nodeOptions: DropdownOption[] = useMemo(() => {
    const base: DropdownOption[] = [{ value: "", label: "Cluster" }];
    const options: DropdownOption[] = nodesSummary.nodeStatuses
      .map(ns => ({
        value: ns.desc.node_id.toString(),
        label: nodesSummary.nodeDisplayNameByID[ns.desc.node_id] as string,
      }))
      .sort((a: DropdownOption, b: DropdownOption) => {
        const aDecommissioned = a.label.startsWith("[decommissioned]");
        const bDecommissioned = b.label.startsWith("[decommissioned]");
        if (aDecommissioned && !bDecommissioned) return 1;
        if (!aDecommissioned && bDecommissioned) return -1;
        return 0;
      });
    return base.concat(options);
  }, [nodesSummary.nodeStatuses, nodesSummary.nodeDisplayNameByID]);
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
          setMetricsFixedWindow={tw => dispatch(setMetricsFixedWindow(tw))}
          setTimeScale={ts => dispatch(setTimeScale(ts))}
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

export default withRouter<
  GraphContainerOwnProps,
  React.ComponentType<GraphContainerOwnProps>
>(GraphContainer);
