export interface Dashboard {
  title: string;
  charts: Chart[];
}

export interface Chart {
  title: string;
  tooltip: string;
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
