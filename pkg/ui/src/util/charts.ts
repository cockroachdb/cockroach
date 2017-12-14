export type Measure = "count" | "bytes" | "duration";

type Metric = {
  name: string,
  title?: string,
};

type MetricsChart = {
  type: "metrics",
  measure: Measure,
  metrics: Metric[],
};

type NodesChart = {
  type: "nodes",
  measure: Measure,
  metric: Metric,
};

type Chart = MetricsChart | NodesChart;

export type ChartConfig = {
  [key: string]: Chart,
};

export function isMetricsChart(chart: Chart): chart is MetricsChart {
  return chart.type === "metrics";
}

export function isNodesChart(chart: Chart): chart is NodesChart {
  return chart.type === "nodes";
}
