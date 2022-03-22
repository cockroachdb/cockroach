// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { shallow } from "enzyme";
import React from "react";
import "src/enzymeInit";
import * as sinon from "sinon";
import uPlot from "uplot";
import _ from "lodash";

import { fillGaps, LineGraph, LineGraphProps } from "./index";
import * as timewindow from "src/redux/timeScale";
import * as protos from "src/js/protos";
import { Axis } from "src/views/shared/components/metricQuery";
import {
  calculateXAxisDomain,
  calculateYAxisDomain,
  configureUPlotLineChart,
} from "src/views/cluster/util/graphs";
import Long from "long";

describe("<LineGraph>", function() {
  let spy: sinon.SinonSpy;
  let mockProps: LineGraphProps;
  const linegraph = (props: LineGraphProps) =>
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
      data: { results: [], toJSON: sinon.spy },
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
          state: sinon.spy,
          hash: "",
        },
        push: () => {},
        replace: () => {},
        go: () => {},
        goBack: () => {},
        goForward: () => {},
        block: sinon.spy,
        listen: sinon.spy,
        createHref: () => {
          return "";
        },
      },
    };
    spy = sinon.spy();
  });

  it("should render a root component on mount", () => {
    const wrapper = linegraph({ ...mockProps });
    const root = wrapper.find(".linegraph");
    assert.equal(root.length, 1);
  });

  it("should set new history", () => {
    const wrapper = linegraph({
      ...mockProps,
      history: { ...mockProps.history, push: spy },
    });
    const instance = (wrapper.instance() as any) as LineGraph;
    instance.setNewTimeRange(111111, 222222);
    assert.isTrue(
      spy.calledWith({ pathname: "", search: "start=111&end=222" }),
    );
  });

  it("should set a new chart on update", () => {
    const wrapper = linegraph({ ...mockProps });
    const instance = (wrapper.instance() as any) as LineGraph;
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
    const result = _.isEmpty(instance.u);
    assert.equal(result, false);
  });

  it("should update the existing chart", () => {
    // test setup
    const wrapper = linegraph({
      ...mockProps,
      data: { results: [{}], toJSON: sinon.spy },
    });
    const instance = (wrapper.instance() as unknown) as LineGraph;
    const mockFn = sinon.spy();
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
    const mockData: protos.cockroach.ts.tspb.TimeSeriesQueryResponse = new protos.cockroach.ts.tspb.TimeSeriesQueryResponse();
    const mockOptions = configureUPlotLineChart(
      mockMetrics,
      mockAxis,
      mockData,
      instance.setNewTimeRange,
      () => calculateYAxisDomain(0, mockData),
      () => calculateXAxisDomain(mockProps.timeInfo),
    );
    instance.u = new uPlot(mockOptions);
    const setDataSpy = sinon.spy(instance.u, "setData");
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
    assert.isTrue(setDataSpy.called);
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
    assert.equal(result.length, 50);
  });
});
