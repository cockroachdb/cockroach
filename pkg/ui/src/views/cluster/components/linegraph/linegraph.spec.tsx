import { assert, expect } from "chai";
import { mount, shallow } from "enzyme";
import React from "react";
import "src/enzymeInit";
import * as sinon from "sinon";
import uPlot from "uplot";
import _ from "lodash";

import { LineGraph, LineGraphProps } from "./index";
import * as timewindow from "src/redux/timewindow";
import { Axis } from "src/views/shared/components/metricQuery";
import {
  calculateXAxisDomain,
  calculateYAxisDomain,
  configureUPlotLineChart,
} from "src/views/cluster/util/graphs";
import Long from "long";

describe("<LineGraph>", function () {
  let spy: sinon.SinonSpy;
  let mockProps: LineGraphProps;
  const linegraph = (props: LineGraphProps) =>
    shallow(
      <LineGraph {...props}>
        <Axis />
      </LineGraph>,
    );

  beforeEach(function () {
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
      setTimeRange: timewindow.setTimeRange,
      setTimeScale: timewindow.setTimeScale,
      history: { location: { pathname: "", search: "" }, push: () => {} },
    };
    spy = sinon.spy();
  });

  it("should render a root component on mount", () => {
    const wrapper = linegraph({ ...mockProps });
    const root = wrapper.find(".linegraph");
    assert.equal(root.length, 1);
  }),
    it("should set new history", () => {
      const wrapper = linegraph({
        ...mockProps,
        history: { ...mockProps.history, push: spy },
      });
      const instance = (wrapper.instance() as unknown) as LineGraph;
      instance.setNewTimeRange(111111, 222222);
      assert.isTrue(
        spy.calledWith({ pathname: "", search: "start=111&end=222" }),
      );
    });
  it("should set a new chart on update", () => {
    const wrapper = linegraph({ ...mockProps });
    const instance = (wrapper.instance() as unknown) as LineGraph;
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
    const wrapper = linegraph({ ...mockProps });
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
      key: ".0",
      props: { label: "queries", children: [{}], units: 0 },
      _owner: {},
      _store: { validated: false },
    };
    const mockData = {
      results: [
        {
          datapoints: [
            {
              timestamp_nanos: {
                low: -900000000,
                high: 900000000,
                unsigned: false,
              },
              value: 0.7,
            },
          ],
        },
      ],
    };
    const mockCurEl = wrapper.find(".linegraph").childAt(0);
    console.log(mockCurEl);
    const mockOptions = configureUPlotLineChart(
      mockMetrics,
      mockAxis,
      mockData,
      instance.setNewTimeRange,
      () => calculateYAxisDomain(0, mockData),
      () => calculateXAxisDomain(mockProps.timeInfo),
    );
    instance.u = new uPlot(mockOptions);
    instance.u.setData = sinon.spy();
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
    assert.isTrue(instance.u.setData.called);
  });
});
