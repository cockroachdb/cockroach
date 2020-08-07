// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf, RenderFunction } from "@storybook/react";

import {countBarChart, genericBarChart, latencyBarChart, retryBarChart, rowsBarChart} from "./barCharts";
import statementsPagePropsFixture from "./statementsPage.fixture";
import {cockroach} from "src/js/protos";
import NumericStat = cockroach.sql.NumericStat;
import Long from "long";

const { statements } = statementsPagePropsFixture;

const withinColumn = (width = "150px") => (storyFn: RenderFunction) => {
  const rowStyle = {
    borderTop: "1px solid #e7ecf3",
    borderBottom: "1px solid #e7ecf3",
  };

  const cellStyle = {
    width: "190px",
    padding: "10px 20px",
  };

  return (
    <table>
      <tbody>
      <tr style={rowStyle}>
        <td style={cellStyle}>
          <div style={{ width }}>
            { storyFn() }
          </div>
        </td>
      </tr>
      </tbody>
    </table>
  );
};

storiesOf("BarCharts", module)
  .add("countBarChart", () => {
    const chartFactory = countBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("latencyBarChart", () => {
    const chartFactory = latencyBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("retryBarChart", () => {
    const chartFactory = retryBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("rowsBarChart", () => {
    const chartFactory = rowsBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("genericBarChart", () => {
    const stat = new NumericStat();
    stat.mean = 25;
    stat.squared_diffs = 25;
    const barChartFactory = genericBarChart(stat, Long.fromInt(10));
    return barChartFactory();
  })
  .add("genericBarChart (missing data)", () => {
    const barChartFactory = genericBarChart(null, Long.fromInt(10));
    return barChartFactory();
  });

storiesOf("BarCharts/within column (150px)", module)
  .addDecorator(withinColumn())
  .add("countBarChart", () => {
    const chartFactory = countBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("latencyBarChart", () => {
    const chartFactory = latencyBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("retryBarChart", () => {
    const chartFactory = retryBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("rowsBarChart", () => {
    const chartFactory = rowsBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("genericBarChart", () => {
    const stat = new NumericStat();
    stat.mean = 25;
    stat.squared_diffs = 25;
    const barChartFactory = genericBarChart(stat, Long.fromInt(10));
    return barChartFactory();
  });
