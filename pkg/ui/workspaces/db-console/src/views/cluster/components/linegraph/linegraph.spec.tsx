// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import Long from "long";
import React from "react";
import uPlot from "uplot";

import * as timewindow from "src/redux/timeScale";
import { Axis } from "src/views/shared/components/metricQuery";

import { fillGaps, InternalLineGraph, LineGraphProps } from "./index";

// Mock uPlot — JSDOM can't render real charts, so we verify the
// component drives uPlot correctly via constructor and method calls.
jest.mock("uplot", () => {
  return jest.fn().mockImplementation(() => ({
    setData: jest.fn(),
    destroy: jest.fn(),
    axes: [{}, { label: "" }],
  }));
});

// Mock chart helpers to avoid needing the full metrics pipeline.
// formatMetricData returns a stable key (".0") so the effect can
// distinguish "same series, new data" from "new series".
jest.mock("src/views/cluster/util/graphs", () => ({
  ...jest.requireActual("src/views/cluster/util/graphs"),
  configureUPlotLineChart: jest.fn().mockReturnValue({}),
  formatMetricData: jest.fn().mockReturnValue([{ key: ".0", values: [] }]),
}));

const MockUPlot = uPlot as unknown as jest.Mock;

describe("<LineGraph>", function () {
  let mockProps: LineGraphProps;

  beforeEach(() => {
    MockUPlot.mockClear();

    mockProps = {
      title: "Test Title",
      subtitle: "Test Subtitle",
      legend: false,
      xAxis: true,
      data: { results: [] },
      timeInfo: {
        start: new Long(12345),
        end: new Long(2346),
        sampleDuration: new Long(1),
      },
      setMetricsFixedWindow: timewindow.setMetricsFixedWindow,
      setTimeScale: timewindow.setTimeScale,
      timezone: "UTC",
      history: {
        length: 0,
        action: "PUSH" as const,
        location: {
          pathname: "",
          search: "",
          state: jest.fn(),
          hash: "",
        },
        push: jest.fn(),
        replace: jest.fn(),
        go: jest.fn(),
        goBack: jest.fn(),
        goForward: jest.fn(),
        block: jest.fn(),
        listen: jest.fn(),
        createHref: () => "",
      },
    };
  });

  it("should render a root component on mount", () => {
    const { container } = render(
      <InternalLineGraph {...mockProps}>
        <Axis />
      </InternalLineGraph>,
    );
    expect(container.querySelector(".linegraph")).not.toBeNull();
  });

  it("should display an empty state if viewing a virtual cluster and there is no data", () => {
    const { container } = render(
      <InternalLineGraph {...mockProps} tenantSource="demoapp">
        <Axis />
      </InternalLineGraph>,
    );
    expect(container.querySelector(".linegraph-empty")).not.toBeNull();
  });

  it("should create a chart when data with datapoints arrives", () => {
    render(
      <InternalLineGraph
        {...mockProps}
        data={{
          results: [
            {
              datapoints: [
                {
                  timestamp_nanos: new Long(111111, 999999),
                  value: 123456,
                },
              ],
            },
          ],
        }}
      >
        <Axis />
      </InternalLineGraph>,
    );

    expect(MockUPlot).toHaveBeenCalled();
  });

  it("should update the existing chart when data changes", () => {
    const initialData = {
      results: [
        {
          datapoints: [
            {
              timestamp_nanos: new Long(111111, 999999),
              value: 100,
            },
          ],
        },
      ],
    };

    const { rerender } = render(
      <InternalLineGraph {...mockProps} data={initialData}>
        <Axis />
      </InternalLineGraph>,
    );

    // First render should create the chart.
    expect(MockUPlot).toHaveBeenCalledTimes(1);
    const chartInstance = MockUPlot.mock.results[0].value;

    // Rerender with a new data object (same series structure).
    rerender(
      <InternalLineGraph
        {...mockProps}
        data={{
          results: [
            {
              datapoints: [
                {
                  timestamp_nanos: new Long(222222, 999999),
                  value: 200,
                },
              ],
            },
          ],
        }}
      >
        <Axis />
      </InternalLineGraph>,
    );

    // Should update existing chart via setData, not create a new one.
    expect(chartInstance.setData).toHaveBeenCalled();
  });
});

describe("fillGaps", () => {
  it("fills gaps with missed points", () => {
    const sampleDuration = Long.fromNumber(10000);
    const data: uPlot.AlignedData[0] = [
      1634735320000,
      1634735330000,
      1634735340000,
      1634735350000,
      1634735360000,
      1634735370000,
      1634735380000,
      1634735390000, // missed 39 points after
      1634735780000,
      1634735790000,
      1634735800000, // missed 1 data point after
      1634735810000,
    ];
    const result = fillGaps(data, sampleDuration);
    expect(result.length).toBe(50);
  });
});
