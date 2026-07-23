// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  util,
  findClosestTimeScale,
  defaultTimeScaleOptions,
} from "@cockroachlabs/cluster-ui";
import { render } from "@testing-library/react";
import map from "lodash/map";
import Long from "long";
import moment from "moment-timezone";
import React from "react";
import * as reactRedux from "react-redux";

import { UseMetricsResult } from "src/hooks/useMetrics";
import * as protos from "src/js/protos";
import { adjustTimeScale } from "src/redux/timeScale";
import {
  Axis,
  Metric,
  MetricsDataComponentProps,
  QueryTimeInfo,
} from "src/views/shared/components/metricQuery";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

// Mock react-redux hooks so the component can render without a real store.
// useSelector now returns metricsTime (the raw time state).
const mockDispatch = jest.fn();
jest.mock("react-redux", () => ({
  ...jest.requireActual("react-redux"),
  useSelector: jest.fn(),
  useDispatch: () => mockDispatch,
}));

// Mock useMetrics so we can control what data the component receives and
// verify what request it passes to the hook.
const mockUseMetrics = jest.fn<UseMetricsResult, [any]>();
jest.mock("src/hooks/useMetrics", () => ({
  useMetrics: (req: any) => mockUseMetrics(req),
}));

// Mock useClusterSettings — return empty settings so TTL values are
// undefined. This is sufficient for testing the data flow.
jest.mock("@cockroachlabs/cluster-ui", () => ({
  ...jest.requireActual("@cockroachlabs/cluster-ui"),
  useClusterSettings: (): {
    settingValues: Record<string, never>;
    isLoading: boolean;
    error: Error | undefined;
  } => ({
    settingValues: {},
    isLoading: false,
    error: undefined,
  }),
}));

// TextGraph captures the props that MetricsDataProvider passes to its child
// via React.cloneElement, allowing tests to assert on the data flow.
let capturedProps: MetricsDataComponentProps | null = null;

function TextGraph(props: React.PropsWithChildren<MetricsDataComponentProps>) {
  capturedProps = props;
  return (
    <div>
      {props.data && props.data.results ? props.data.results.join(":") : ""}
    </div>
  );
}

const childrenJSX = (
  <TextGraph>
    <Axis>
      <Metric name="test.metric.1" />
      <Metric name="test.metric.2" />
    </Axis>
    <Axis>
      <Metric name="test.metric.3" />
    </Axis>
  </TextGraph>
);

// Create a metricsTime-shaped object with a currentWindow that will
// produce a deterministic timeInfo through the inline computation.
function createMetricsTime(startMs: number, endMs: number) {
  return {
    currentWindow: {
      start: moment(startMs),
      end: moment(endMs),
    },
  };
}

// Compute the expected timeInfo for a given metricsTime, matching the
// component's inline computation with undefined TTL values.
function computeExpectedTimeInfo(
  startMs: number,
  endMs: number,
): QueryTimeInfo {
  const syncedScale = findClosestTimeScale(
    defaultTimeScaleOptions,
    util.MilliToSeconds(endMs - startMs),
  );
  const adjusted = adjustTimeScale(
    { ...syncedScale, fixedWindowEnd: false },
    { start: moment(startMs), end: moment(endMs) },
    undefined,
    undefined,
  );
  return {
    start: Long.fromNumber(util.MilliToNano(startMs)),
    end: Long.fromNumber(util.MilliToNano(endMs)),
    sampleDuration: Long.fromNumber(
      util.MilliToNano(adjusted.timeScale.sampleSize.asMilliseconds()),
    ),
  };
}

function mockTimeInfo(metricsTimeVal: any) {
  (reactRedux.useSelector as jest.Mock).mockReturnValue(metricsTimeVal);
}

function makeMetricsRequest(
  timeInfo: QueryTimeInfo,
  sources?: string[],
  tenantSource?: string,
) {
  return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
    start_nanos: timeInfo.start,
    end_nanos: timeInfo.end,
    sample_nanos: timeInfo.sampleDuration,
    queries: [
      {
        name: "test.metric.1",
        sources: sources,
        tenant_id: tenantSource
          ? { id: Long.fromString(tenantSource) }
          : undefined,
        downsampler: protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX,
        source_aggregator:
          protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.SUM,
        derivative: protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.NONE,
      },
      {
        name: "test.metric.2",
        sources: sources,
        tenant_id: tenantSource
          ? { id: Long.fromString(tenantSource) }
          : undefined,
        downsampler: protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX,
        source_aggregator:
          protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.SUM,
        derivative: protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.NONE,
      },
      {
        name: "test.metric.3",
        sources: sources,
        tenant_id: tenantSource
          ? { id: Long.fromString(tenantSource) }
          : undefined,
        downsampler: protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX,
        source_aggregator:
          protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.SUM,
        derivative: protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.NONE,
      },
    ],
  });
}

function makeResponse(timeInfo: QueryTimeInfo) {
  const request = makeMetricsRequest(timeInfo);
  return new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
    results: map(request.queries, q => ({
      query: q,
      datapoints: [],
    })),
  });
}

const noData: UseMetricsResult = {
  data: undefined,
  error: undefined,
  isLoading: true,
  isValidating: true,
};

function withData(timeInfo: QueryTimeInfo): UseMetricsResult {
  return {
    data: makeResponse(timeInfo),
    error: undefined,
    isLoading: false,
    isValidating: false,
  };
}

describe("<MetricsDataProvider>", () => {
  // Use realistic millisecond values for constructing metricsTime objects.
  // 10-minute window starting from epoch.
  const start1Ms = 0;
  const end1Ms = 10 * 60 * 1000;
  const metricsTime1 = createMetricsTime(start1Ms, end1Ms);
  const timespan1 = computeExpectedTimeInfo(start1Ms, end1Ms);

  const start2Ms = 10 * 60 * 1000;
  const end2Ms = 20 * 60 * 1000;
  const metricsTime2 = createMetricsTime(start2Ms, end2Ms);
  const timespan2 = computeExpectedTimeInfo(start2Ms, end2Ms);

  const graphid = "testgraph";

  beforeEach(() => {
    mockDispatch.mockReset();
    mockUseMetrics.mockReset();
    (reactRedux.useSelector as jest.Mock).mockReset();
    capturedProps = null;
  });

  describe("request construction", () => {
    it("passes correct request to useMetrics on mount", () => {
      mockTimeInfo(metricsTime1);
      mockUseMetrics.mockReturnValue(noData);
      render(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(mockUseMetrics).toHaveBeenCalledWith(
        makeMetricsRequest(timespan1),
      );
    });

    it("passes undefined request when timeInfo is null", () => {
      mockTimeInfo({ currentWindow: null });
      mockUseMetrics.mockReturnValue(noData);
      render(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(mockUseMetrics).toHaveBeenCalledWith(undefined);
    });

    it("passes updated request when timespan changes", () => {
      mockTimeInfo(metricsTime1);
      mockUseMetrics.mockReturnValue(noData);
      const { rerender } = render(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(mockUseMetrics).toHaveBeenLastCalledWith(
        makeMetricsRequest(timespan1),
      );

      // Rerender with a different timespan.
      mockTimeInfo(metricsTime2);
      rerender(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(mockUseMetrics).toHaveBeenLastCalledWith(
        makeMetricsRequest(timespan2),
      );
    });
  });

  describe("attach", () => {
    it("attaches metrics data to contained component", () => {
      mockTimeInfo(metricsTime1);
      mockUseMetrics.mockReturnValue(withData(timespan1));
      render(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(capturedProps.data).toBeDefined();
      expect(capturedProps.data).toEqual(makeResponse(timespan1));
    });

    it("passes undefined data when loading", () => {
      mockTimeInfo(metricsTime1);
      mockUseMetrics.mockReturnValue(noData);
      render(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(capturedProps.data).toBeUndefined();
    });

    it("passes timeInfo to contained component", () => {
      mockTimeInfo(metricsTime1);
      mockUseMetrics.mockReturnValue(noData);
      render(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(capturedProps.timeInfo).toEqual(timespan1);
    });

    it("throws error if it contains multiple graph components", () => {
      // Suppress React error boundary console output for expected throw.
      const consoleSpy = jest
        .spyOn(console, "error")
        .mockImplementation(() => {});
      mockTimeInfo(metricsTime1);
      mockUseMetrics.mockReturnValue(noData);
      expect(() => {
        render(
          // @ts-expect-error: intentionally passing multiple children to test runtime error
          <MetricsDataProvider id="id">
            <TextGraph>
              <Axis>
                <Metric name="test.metrics.1" />
              </Axis>
            </TextGraph>
            <TextGraph>
              <Axis>
                <Metric name="test.metrics.2" />
              </Axis>
            </TextGraph>
          </MetricsDataProvider>,
        );
      }).toThrow();
      consoleSpy.mockRestore();
    });
  });
});
