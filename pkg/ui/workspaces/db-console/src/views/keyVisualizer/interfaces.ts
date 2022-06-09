export interface Sample {
  sampleTime: { wallTime: number };
  spanStats: SpanStatistics[];
}

export interface SpanStatistics {
  // pretty
  span: { startKey: string; endKey: string };
  batchRequests: number;
}

export interface GetSamplesResponse {
  samples: Sample[];
  keys: string[]; // lexicographically sorted
}

export interface KeyVisualizerProps {
  response: GetSamplesResponse;

  yOffsetForKey: Record<string, number>;

  highestTemp: number;

  setTooltipDetails: (x: number, y: number, time: string, spanStats: SpanStatistics) => void;

  setShowTooltip: (show: boolean) => void;
}
