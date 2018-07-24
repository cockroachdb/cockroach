import { ComponentType } from "react";

export interface Dashboard {
  title: string;
  charts: Chart[];
}

export interface Chart {
  title: string;
  tooltip: ComponentType<TooltipProps>;
  axis: Axis;
  sourceLevel?: SourceLevel;
  metrics: Metric[];
}

export interface Axis {
  label: string;
  units?: Units;
}

export enum SourceLevel {
  Node = "node",
  Store = "store",
}

export enum Units {
  Count = "count",
  Duration = "duration",
  Bytes = "bytes",
}

export interface Metric {
  title: string;
  name: string;
  tooltip?: string;
  nonNegativeRate?: boolean;
  downsampleMax?: boolean;
  aggregateAvg?: boolean;
}

export interface TooltipProps {
  selection: string;
}
