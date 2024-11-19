// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";
import { AlignedData, Options } from "uplot";

import { AxisUnits } from "../utils/domain";

import { getBarsBuilder } from "./bars";

import { BarGraphTimeSeries } from "./index";

function generateTimestampsMillis(start: number, length: number): number[] {
  return [...Array(length)].map(
    (_, idx): number => (60 * 60 * idx + start) * 1000,
  );
}

function genValuesInRange(range: [number, number], length: number): number[] {
  return [...Array(length)].map((): number =>
    Math.random() > 0.1 ? Math.random() * (range[1] - range[0]) + range[0] : 0,
  );
}

const mockData: AlignedData = [
  generateTimestampsMillis(1546300800, 20),
  genValuesInRange([0, 100], 20),
  genValuesInRange([0, 10000], 20),
  genValuesInRange([0, 100000], 20),
];
const mockDataSingle: AlignedData = [[1654115121], [0], [1], [2]];
const mockDataDuration: AlignedData = [
  generateTimestampsMillis(1546300800, 20),
  genValuesInRange([0, 1e7], 20),
  genValuesInRange([0, 1e7], 20),
  genValuesInRange([0, 1e7], 20),
];

const mockOpts: Partial<Options> = {
  axes: [{}, { label: "values" }],
  series: [
    {},
    {
      label: "bar 1",
    },
    {
      label: "bar 2",
    },
    {
      label: "bar 3",
    },
  ],
};

storiesOf("BarGraphTimeSeries", module)
  .add("with stacked multi-series", () => {
    return (
      <BarGraphTimeSeries
        title="Example Stacked - Count"
        alignedData={mockData}
        uPlotOptions={mockOpts}
        tooltip={
          <div>This is an example stacked bar graph axis unit = Count.</div>
        }
        yAxisUnits={AxisUnits.Count}
      />
    );
  })
  .add("with single series", () => {
    const data: AlignedData = [
      generateTimestampsMillis(1546300800, 50),
      genValuesInRange([0, 1], 50),
    ];
    const opts = {
      series: [{}, { label: "bar", paths: getBarsBuilder(0.8, 20) }],
      legend: { show: false },
      axes: [{}, { label: "mock" }],
    };
    return (
      <BarGraphTimeSeries
        title="Example Single Series - Percent"
        alignedData={data}
        uPlotOptions={opts}
        tooltip={
          <div>This is an example bar graph with axis unit = percent.</div>
        }
        yAxisUnits={AxisUnits.Percentage}
      />
    );
  })
  .add("with single stacked multi-series", () => {
    return (
      <BarGraphTimeSeries
        title="Example one Stacked - Count"
        alignedData={mockDataSingle}
        uPlotOptions={mockOpts}
        tooltip={
          <div>This is an example stacked bar graph axis unit = Count.</div>
        }
        yAxisUnits={AxisUnits.Count}
      />
    );
  })
  .add("with duration stacked multi-series", () => {
    return (
      <BarGraphTimeSeries
        title="Example one Stacked - Duration"
        alignedData={mockDataDuration}
        uPlotOptions={mockOpts}
        tooltip={
          <div>This is an example stacked bar graph axis unit = Duration.</div>
        }
        yAxisUnits={AxisUnits.Duration}
      />
    );
  })
  .add("with bytes stacked multi-series", () => {
    return (
      <BarGraphTimeSeries
        title="Example one Stacked - Bytes"
        alignedData={mockDataDuration}
        uPlotOptions={mockOpts}
        tooltip={
          <div>This is an example stacked bar graph axis unit = Bytes.</div>
        }
        yAxisUnits={AxisUnits.Bytes}
      />
    );
  });
