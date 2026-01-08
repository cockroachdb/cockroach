// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TimeScale } from "@cockroachlabs/cluster-ui";
import { Tabs, Upload } from "antd";
import { RcFile } from "antd/es/upload";
import React, { useState, useEffect, useRef } from "react";
import { connect } from "react-redux";
import { withRouter } from "react-router-dom";

import { getCookieValue } from "oss/src/redux/cookies";
import { AdminUIState } from "oss/src/redux/state";
import TimeScaleDropdown from "oss/src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import { Button } from "src/components/button";
import {
  refreshMetricMetadata,
  refreshNodes,
  refreshTenantsList,
} from "src/redux/apiReducers";
import {
  setMetricsFixedWindow,
  selectTimeScale,
  setTimeScale,
} from "src/redux/timeScale";

import DashboardTab from "./components/dashboardTab";
import { DashboardConfig } from "./dashboardConfig";

import type { TabsProps } from "antd";

import "./metricsWorkspace.styl";

type Props = {
  timeScale: TimeScale;
  setTimeScale: (timeScale: TimeScale) => void;
  refreshNodes: typeof refreshNodes;
  refreshMetricMetadata: typeof refreshMetricMetadata;
  refreshTenantsList: typeof refreshTenantsList;
};
const MetricsWorkspace = ({
  timeScale,
  setTimeScale,
  refreshNodes,
  refreshMetricMetadata,
  refreshTenantsList,
}: Props) => {
  const dashboardCount = useRef(1);
  const [dashboardTabs, setDashboardTabs] = useState<DashboardConfig[]>([]);
  const [activeTabKey, setActiveTabKey] = useState<string>();
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    refreshNodes();
    refreshMetricMetadata();
    refreshTenantsList();
  }, [refreshNodes, refreshMetricMetadata, refreshTenantsList]);

  const handleCreateNewDashboard = () => {
    const name = `Dashboard ${dashboardCount.current}`;
    dashboardCount.current++;
    const newTab: DashboardConfig = {
      key: name,
      name,
      graphs: [],
    };
    setDashboardTabs(prev => [...prev, newTab]);
    setActiveTabKey(newTab.key as string);
  };

  const handleLoadDashboard = (file: RcFile) => {
    setLoadError(null);
    const reader = new FileReader();
    reader.onload = e => {
      try {
        const config: DashboardConfig = JSON.parse(e.target?.result as string);
        const fileName = (file as any).name || "dashboard";
        // Use timestamp and index to create a unique key, avoiding file names
        const uniqueKey = `dashboard-${Date.now()}`;
        const newTab: DashboardConfig = {
          key: uniqueKey,
          name: config.name || fileName.replace(".json", ""),
          graphs: config.graphs || [],
        };
        setDashboardTabs(prev => [...prev, newTab]);
        setActiveTabKey(newTab.key as string);
        setLoadError(null);
      } catch (error) {
        setLoadError(
          "Failed to parse dashboard file. Please ensure it's valid JSON.",
        );
      }
    };
    reader.onerror = () => {
      setLoadError("Failed to read file. Please try again.");
    };
    reader.readAsText(file as any);
    // Return false to prevent default upload behavior
    return false;
  };

  const handleTabEdit = (
    targetKey:
      | string
      | React.MouseEvent<Element, MouseEvent>
      | React.KeyboardEvent<Element>,
    action: "add" | "remove",
  ) => {
    switch (action) {
      case "add":
        handleCreateNewDashboard();
        break;
      case "remove":
        setDashboardTabs(prev => prev.filter(tab => tab.key !== targetKey));
        if (activeTabKey === targetKey) {
          const remainingTabs = dashboardTabs.filter(
            tab => tab.key !== targetKey,
          );
          setActiveTabKey(
            remainingTabs.length > 0
              ? (remainingTabs[0].key as string)
              : undefined,
          );
        }
        break;
      default:
        break;
    }
  };

  const handleDashboardChange = (
    dashboardKey: string,
    updater: (dashboard: DashboardConfig) => DashboardConfig,
  ) => {
    setDashboardTabs(prev =>
      prev.map(tab => (tab.key === dashboardKey ? updater(tab) : tab)),
    );
  };

  const tabItems: TabsProps["items"] = dashboardTabs.map(tab => ({
    key: tab.key,
    label: tab.name,
    children: (
      <DashboardTab
        config={tab}
        onDashboardChange={(
          updater: (dashboard: DashboardConfig) => DashboardConfig,
        ) => handleDashboardChange(tab.key as string, updater)}
      />
    ),
  }));

  return (
    <div className="metrics-workspace">
      <div className="metrics-workspace__header">
        <h1 className="metrics-workspace__title">Metrics Workspace</h1>
        <div className="metrics-workspace__header-actions">
          <Button type="primary" onClick={handleCreateNewDashboard}>
            Create New Dashboard
          </Button>
          <div>
            <Upload
              accept=".json"
              beforeUpload={handleLoadDashboard}
              capture="file"
              showUploadList={false}
            >
              <Button type="secondary">Load Dashboard</Button>
            </Upload>
            {loadError && (
              <div
                style={{
                  color: "#ff4d4f",
                  fontSize: "12px",
                  marginTop: "8px",
                }}
              >
                {loadError}
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="metrics-workspace__time-selector">
        <TimeScaleDropdown
          currentScale={timeScale}
          setTimeScale={setTimeScale}
        />
      </div>

      {dashboardTabs.length > 0 ? (
        <Tabs
          type="editable-card"
          activeKey={activeTabKey}
          onChange={setActiveTabKey}
          onEdit={handleTabEdit}
          items={tabItems}
          destroyInactiveTabPane={true}
        />
      ) : (
        <div className="metrics-workspace__empty-state">
          <p>No dashboards loaded.</p>
          <p>
            Create a new dashboard or load one from a JSON file to get started.
          </p>
        </div>
      )}
    </div>
  );
};

const mapStateToProps = (state: AdminUIState) => ({
  timeScale: selectTimeScale(state),
  currentTenant: getCookieValue("tenant"),
});

const mapDispatchToProps = {
  setMetricsFixedWindow: setMetricsFixedWindow,
  setTimeScale: setTimeScale,
  refreshNodes,
  refreshMetricMetadata,
  refreshTenantsList,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(MetricsWorkspace),
);
