// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf } from "@storybook/react";
import { AlignedData, Options } from "uplot";
import { BarGraphTimeSeries } from "./index";
import { AxisUnits } from "../utils/domain";
import { getBarsBuilder } from "./bars";

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
  genValuesInRange([0, 100], 20),
  genValuesInRange([0, 100], 20),
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
          <div>This is an example stacked bar graph axis unit = count.</div>
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
  });
