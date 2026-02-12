// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import map from "lodash/map";
import Long from "long";
import React from "react";
import * as reactRedux from "react-redux";

import * as protos from "src/js/protos";
import { MetricsQuery, requestMetrics } from "src/redux/metrics";
import {
  Axis,
  Metric,
  MetricsDataComponentProps,
  QueryTimeInfo,
} from "src/views/shared/components/metricQuery";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

// Mock react-redux hooks so the component can render without a real store.
// useSelector is mocked per-test to return controlled metrics and timeInfo.
// useDispatch returns a jest.fn() to capture dispatched actions.
const mockDispatch = jest.fn();
jest.mock("react-redux", () => ({
  ...jest.requireActual("react-redux"),
  useSelector: jest.fn(),
  useDispatch: () => mockDispatch,
}));

// Spy on requestMetrics so we can verify the action creator was called
// with the right arguments, independent of the dispatch mock.
jest.mock("src/redux/metrics", () => {
  const actual = jest.requireActual("src/redux/metrics");
  return {
    ...actual,
    requestMetrics: jest.fn(actual.requestMetrics),
  };
});

// Mock refreshSettings to return a simple action (avoid loading the full
// apiReducers dependency tree).
jest.mock("src/redux/apiReducers", () => ({
  ...jest.requireActual("src/redux/apiReducers"),
  refreshSettings: jest.fn(() => ({ type: "MOCK_REFRESH_SETTINGS" })),
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

/**
 * Set up useSelector to return the given metrics and timeInfo values.
 * The component calls useSelector twice per render:
 *   1. state => state.metrics.queries[id]  — returns metrics
 *   2. state => timeInfoSelector(state)    — returns timeInfo
 */
function mockSelectorValues(
  metricsVal: MetricsQuery | null | undefined,
  timeInfoVal: QueryTimeInfo,
) {
  (reactRedux.useSelector as jest.Mock)
    .mockReturnValueOnce(metricsVal)
    .mockReturnValueOnce(timeInfoVal);
}

function renderDataProvider(
  id: string,
  metrics: MetricsQuery | null | undefined,
  timeInfo: QueryTimeInfo,
) {
  capturedProps = null;
  mockSelectorValues(metrics, timeInfo);
  return render(
    <MetricsDataProvider id={id}>{childrenJSX}</MetricsDataProvider>,
  );
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

function makeMetricsQuery(
  id: string,
  timeSpan: QueryTimeInfo,
  sources?: string[],
  tenantSource?: string,
): MetricsQuery {
  const request = makeMetricsRequest(timeSpan, sources, tenantSource);
  const data = new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
    results: map(request.queries, q => {
      return {
        query: q,
        datapoints: [],
      };
    }),
  });
  return {
    id,
    request,
    data,
    nextRequest: request,
    error: undefined,
  };
}

describe("<MetricsDataProvider>", () => {
  const timespan1: QueryTimeInfo = {
    start: Long.fromNumber(0),
    end: Long.fromNumber(100),
    sampleDuration: Long.fromNumber(300),
  };
  const timespan2: QueryTimeInfo = {
    start: Long.fromNumber(100),
    end: Long.fromNumber(200),
    sampleDuration: Long.fromNumber(300),
  };
  const graphid = "testgraph";

  beforeEach(() => {
    mockDispatch.mockReset();
    (requestMetrics as jest.Mock).mockClear();
    (reactRedux.useSelector as jest.Mock).mockReset();
    capturedProps = null;
  });

  describe("refresh", () => {
    it("refreshes query data when mounted", () => {
      renderDataProvider(graphid, null, timespan1);
      expect(requestMetrics).toHaveBeenCalledWith(
        graphid,
        makeMetricsRequest(timespan1),
      );
    });

    it("does nothing when mounted if current request fulfilled", () => {
      renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1),
        timespan1,
      );
      expect(requestMetrics).not.toHaveBeenCalled();
    });

    it("does nothing when mounted if current request is in flight", () => {
      const query = makeMetricsQuery(graphid, timespan1);
      query.request = null;
      query.data = null;
      renderDataProvider(graphid, query, timespan1);
      expect(requestMetrics).not.toHaveBeenCalled();
    });

    it("refreshes query data when receiving props", () => {
      const { rerender } = renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1),
        timespan1,
      );
      expect(requestMetrics).not.toHaveBeenCalled();
      // Rerender with metrics cleared — triggers a refresh.
      mockSelectorValues(undefined, timespan1);
      rerender(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(requestMetrics).toHaveBeenCalledWith(
        graphid,
        makeMetricsRequest(timespan1),
      );
    });

    it("refreshes if timespan changes", () => {
      const { rerender } = renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1),
        timespan1,
      );
      expect(requestMetrics).not.toHaveBeenCalled();
      // Rerender with a different timespan.
      mockSelectorValues(makeMetricsQuery(graphid, timespan1), timespan2);
      rerender(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(requestMetrics).toHaveBeenCalledWith(
        graphid,
        makeMetricsRequest(timespan2),
      );
    });

    it("refreshes if query changes", () => {
      const { rerender } = renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1),
        timespan1,
      );
      expect(requestMetrics).not.toHaveBeenCalled();
      // Rerender with different sources in the metrics query.
      mockSelectorValues(
        makeMetricsQuery(graphid, timespan1, ["1"]),
        timespan1,
      );
      rerender(
        <MetricsDataProvider id={graphid}>{childrenJSX}</MetricsDataProvider>,
      );
      expect(requestMetrics).toHaveBeenCalledWith(
        graphid,
        makeMetricsRequest(timespan1),
      );
    });
  });

  describe("attach", () => {
    it("attaches metrics data to contained component", () => {
      renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1),
        timespan1,
      );
      expect(capturedProps.data).toBeDefined();
      expect(capturedProps.data).toEqual(
        makeMetricsQuery(graphid, timespan1).data,
      );
    });

    it("attaches metrics data if timespan doesn't match", () => {
      renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1),
        timespan2,
      );
      expect(capturedProps.data).toBeDefined();
      expect(capturedProps.data).toEqual(
        makeMetricsQuery(graphid, timespan1).data,
      );
    });

    it("does not attach metrics data if query doesn't match", () => {
      renderDataProvider(
        graphid,
        makeMetricsQuery(graphid, timespan1, ["1"]),
        timespan1,
      );
      expect(capturedProps.data).toBeUndefined();
    });

    it("throws error if it contains multiple graph components", () => {
      // Suppress React error boundary console output for expected throw.
      const consoleSpy = jest
        .spyOn(console, "error")
        .mockImplementation(() => {});
      mockSelectorValues(null, timespan1);
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
