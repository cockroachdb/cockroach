import React from "react";
import { Dashboard, Units, SourceLevel, TooltipProps } from "./interface";

export const overviewDashboard: Dashboard = {
  title: "Overview",
  charts: [
    {
      title: "Statements",
      tooltip: (props: TooltipProps) => (
        <React.Fragment>
          A ten-second moving average of statements completed per second {props.selection}.
        </React.Fragment>
      ),
      axis: {
        label: "queries",
      },
      metrics: [
        {
          title: "Reads",
          name: "cr.node.sql.select.count",
          tooltip: "The SELECT statements.",
          nonNegativeRate: true,
        },
        {
          title: "Updates",
          name: "cr.node.sql.update.count",
          tooltip: "The UPDATE statements.",
          nonNegativeRate: true,
        },
      ],
    },
    {
      title: "Service Latency",
      tooltip: (props: TooltipProps) => (
        <React.Fragment>
          Over the last minute, 99% of queries ${props.selection} were executed within this time.&nbsp;
          <em>This time does not include network latency between the node and client.</em>
        </React.Fragment>
      ),
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
      tooltip: (_props: TooltipProps) => (<div>"Range replicas per node..."</div>),
      axis: {
        label: "replicas",
      },
      sourceLevel: SourceLevel.Store,
      metrics: [
        {
          title: "Total",
          name: "cr.store.replicas",
          tooltip: "The total number of replicas",
        },
        {
          title: "Quiescent",
          name: "cr.store.replicas.quiescent",
          tooltip: "Replicas that are not currently heartbeating.",
        },
      ],
    },
    {
      title: "Capacity",
      tooltip: (_props: TooltipProps) => (<div>"The disk space available/used..."</div>),
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
