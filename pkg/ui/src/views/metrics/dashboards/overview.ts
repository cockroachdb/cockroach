import { Dashboard, Units, SourceLevel } from "./interface";

export const overviewDashboard: Dashboard = {
  title: "Overview",
  charts: [
    {
      title: "Statements",
      tooltip: "A ten-second moving average...",
      axis: {
        label: "queries",
      },
      metrics: [
        {
          title: "Reads",
          name: "cr.node.sql.select.count",
          tooltip: "Just the SELECTS...",
          nonNegativeRate: true,
        },
        {
          title: "Updates",
          name: "cr.node.sql.update.count",
          tooltip: "Just the UPDATES...",
          nonNegativeRate: true,
        },
      ],
    },
    {
      title: "Service Latency",
      tooltip: "The time taken to parse, plan, and execute statements",
      axis: {
        units: Units.Duration,
        label: "latency",
      },
      metrics: [
        {
          title: "99th percentile",
          name: "cr.node.sql.service.latency-p99",
          downsampleMax: true,
          aggregateAvg: true,
        },
      ],
    },
    {
      title: "Replicas",
      tooltip: "Range replicas per node...",
      axis: {
        label: "replicas",
      },
      sourceLevel: SourceLevel.Store,
      metrics: [
        {
          title: "Total",
          name: "cr.store.replicas",
        },
        {
          title: "Quiescent",
          name: "cr.store.replicas.quiescent",
        },
      ],
    },
    {
      title: "Capacity",
      tooltip: "The disk space available/used...",
      axis: {
        units: Units.Bytes,
        label: "capacity",
      },
      sourceLevel: SourceLevel.Store,
      metrics: [
        {
          title: "Total",
          name: "cr.store.capacity",
        },
        {
          title: "Available",
          name: "cr.store.capacity.available",
        },
        {
          title: "Used",
          name: "cr.store.capacity.used",
        },
      ],
    },
  ],
};
