// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import { Button } from "src/components/button";

import { CustomMetricState } from "../../customChart/customMetric";
import { DashboardConfig, GraphConfig } from "../dashboardConfig";

import EditableTitle from "./editableTitle";
import GraphContainer from "./graphContainer";

interface DashboardTabProps {
  config: DashboardConfig;
  onDashboardChange: (
    updater: (dashboard: DashboardConfig) => DashboardConfig,
  ) => void;
}

const DashboardTab: React.FC<DashboardTabProps> = ({
  config,
  onDashboardChange,
}: DashboardTabProps) => {
  const handleNameChange = (newName: string) => {
    onDashboardChange(dashboard => ({
      ...dashboard,
      name: newName,
    }));
  };

  const handleCreateNewGraph = () => {
    const newMetrics = [new CustomMetricState()];
    onDashboardChange(dashboard => ({
      ...dashboard,
      graphs: [
        ...dashboard.graphs,
        {
          title: "",
          axis: {
            units: AxisUnits.Count,
            label: "",
          },
          metrics: newMetrics,
        },
      ],
    }));
  };

  const handleGraphConfigChange = (
    graphIndex: number,
    newGraphConfig: GraphConfig,
  ) => {
    onDashboardChange(dashboard => ({
      ...dashboard,
      graphs: dashboard.graphs.map((graph, idx) =>
        idx === graphIndex ? newGraphConfig : graph,
      ),
    }));
  };

  const handleGraphDelete = (graphIndex: number) => {
    onDashboardChange(dashboard => ({
      ...dashboard,
      graphs: dashboard.graphs.filter((_, idx) => idx !== graphIndex),
    }));
  };

  const handleExport = () => {
    // Create export config without the internal 'key' field
    const exportConfig = {
      name: config.name,
      graphs: config.graphs,
    };

    const jsonString = JSON.stringify(exportConfig, null, 2);
    const blob = new Blob([jsonString], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `${config.name || "dashboard"}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="metrics-workspace__dashboard-content">
      <div
        style={{
          marginBottom: "24px",
          paddingBottom: "16px",
          borderBottom: "1px solid #e8e8e8",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <EditableTitle
          value={config.name}
          onChange={handleNameChange}
          placeholder="Dashboard Name"
          size="large"
        />
        <Button type="secondary" onClick={handleExport}>
          Export Dashboard
        </Button>
      </div>
      {config.graphs.length > 0 &&
        config.graphs.map((graph, index) => (
          <GraphContainer
            key={index}
            index={index}
            graphConfig={graph}
            onConfigChange={handleGraphConfigChange}
            onDelete={handleGraphDelete}
          />
        ))}
      <div style={{ marginTop: "24px" }}>
        <Button type="primary" onClick={handleCreateNewGraph}>
          Add Graph
        </Button>
      </div>
      {!config.graphs?.length && (
        <div className="metrics-workspace__empty-state">
          No graphs configured. Add graphs to this dashboard.
        </div>
      )}
    </div>
  );
};

export default DashboardTab;
