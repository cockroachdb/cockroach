// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  calculateXAxisDomain,
  calculateYAxisDomain,
  util,
} from "@cockroachlabs/cluster-ui";
import { shallow, ShallowWrapper } from "enzyme";
import flatMap from "lodash/flatMap";
import isEmpty from "lodash/isEmpty";
import Long from "long";
import React from "react";
import uPlot from "uplot";

import * as protos from "src/js/protos";
import * as timewindow from "src/redux/timeScale";
import { configureUPlotLineChart } from "src/views/cluster/util/graphs";
import { Axis } from "src/views/shared/components/metricQuery";

import LineGraph, { fillGaps, InternalLineGraph, OwnProps } from "./index";

describe("<LineGraph>", function () {
  let mockProps: OwnProps;
  const linegraph = (props: OwnProps) =>
    shallow(
      <LineGraph {...props}>
        <Axis />
      </LineGraph>,
    );

  beforeEach(() => {
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
      history: {
        length: 0,
        action: "PUSH",
        location: {
          pathname: "",
          search: "",
          state: jest.fn(),
          hash: "",
        },
        push: () => {},
        replace: () => {},
        go: () => {},
        goBack: () => {},
        goForward: () => {},
        block: jest.fn(),
        listen: jest.fn(),
        createHref: () => {
          return "";
        },
      },
    };
  });

  it("should render a root component on mount", () => {
    const wrapper = linegraph({ ...mockProps });
    const root = wrapper.dive().find(".linegraph");
    expect(root.length).toBe(1);
  });

  it("should display an empty state if viewing a virtual cluster and there is no data", () => {
    mockProps.tenantSource = "demoapp";
    const wrapper = linegraph({ ...mockProps }).dive() as ShallowWrapper<
      any,
      Readonly<{}>,
      React.Component<{}, {}, any>
    >;
    const root = wrapper.find(".linegraph-empty");
    expect(root.length).toBe(1);
  });

  it("should set a new chart on update", () => {
    const wrapper = linegraph({ ...mockProps }).dive() as ShallowWrapper<
      any,
      Readonly<{}>,
      React.Component<{}, {}, any>
    >;
    const instance = wrapper.instance() as any as InternalLineGraph;
    wrapper.setProps({
      data: {
        results: [
          {
            datapoints: [
              {
                timestamp_nanos: {
                  high: 999999,
                  low: 111111,
                  unsigned: false,
                },
                value: 123456,
              },
            ],
          },
        ],
      },
    });
    const result = isEmpty(instance.u);
    expect(result).toEqual(false);
  });

  it("should update the existing chart", () => {
    // test setup
    const wrapper = linegraph({
      ...mockProps,
      data: { results: [{}] },
    }).dive() as ShallowWrapper<
      any,
      Readonly<{}>,
      React.Component<{}, {}, any>
    >;
    const instance = wrapper.instance() as unknown as InternalLineGraph;
    const mockFn = jest.fn();
    const mockMetrics = [
      {
        key: ".0",
        props: { name: "test", nonNegativeRate: true, title: "Selects" },
        type: mockFn,
        _owner: {},
        _store: { validated: false },
      },
    ];
    const mockAxis = {
      type: "AxisProps",
      key: ".0",
      props: { label: "queries", children: [{}], units: 3 },
      _owner: {},
      _store: { validated: false },
    };
    const mockData: protos.cockroach.ts.tspb.TimeSeriesQueryResponse =
      new protos.cockroach.ts.tspb.TimeSeriesQueryResponse();
    const resultDatapoints = flatMap(mockData.results, result =>
      result.datapoints.map(dp => dp.value),
    );
    const mockOptions = configureUPlotLineChart(
      mockMetrics,
      mockAxis,
      mockData,
      instance.setNewTimeRange,
      () => calculateYAxisDomain(0, resultDatapoints),
      () =>
        calculateXAxisDomain(
          util.NanoToMilli(mockProps.timeInfo.start.toNumber()),
          util.NanoToMilli(mockProps.timeInfo.end.toNumber()),
        ),
    );
    instance.u = new uPlot(mockOptions);
    const setDataSpy = jest.spyOn(instance.u, "setData");
    // run test
    wrapper.setProps({
      data: {
        results: [
          {
            datapoints: [
              {
                timestamp_nanos: {
                  high: 999999,
                  low: 111111,
                  unsigned: false,
                },
                value: 123456,
              },
            ],
          },
        ],
      },
    });
    expect(setDataSpy).toHaveBeenCalled();
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
