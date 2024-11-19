// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf, DecoratorFn } from "@storybook/react";
import Long from "long";
import React from "react";

import statementsPagePropsFixture from "src/statementsPage/statementsPage.fixture";

import {
  countBarChart,
  bytesReadBarChart,
  latencyBarChart,
  maxMemUsageBarChart,
  networkBytesBarChart,
  retryBarChart,
} from "./barCharts";

const statements =
  statementsPagePropsFixture.statementsResponse.data.statements;

const withinColumn =
  (width = "150px"): DecoratorFn =>
  storyFn => {
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
              <div style={{ width }}>{storyFn()}</div>
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
  .add("bytesReadBarChart", () => {
    const chartFactory = bytesReadBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("latencyBarChart", () => {
    const chartFactory = latencyBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("maxMemUsageBarChart", () => {
    const chartFactory = maxMemUsageBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("networkBytesBarChart", () => {
    const chartFactory = networkBytesBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("retryBarChart", () => {
    const chartFactory = retryBarChart(statements);
    return chartFactory(statements[0]);
  });

storiesOf("BarCharts/within column (150px)", module)
  .addDecorator(withinColumn())
  .add("countBarChart", () => {
    const chartFactory = countBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("bytesReadBarChart", () => {
    const chartFactory = bytesReadBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("latencyBarChart", () => {
    const chartFactory = latencyBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("maxMemUsageBarChart", () => {
    const chartFactory = maxMemUsageBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("retryBarChart", () => {
    const chartFactory = retryBarChart(statements);
    return chartFactory(statements[0]);
  })
  .add("empty retryBarChart", () => {
    const withoutRetries = statements.map(s => ({
      ...s,
      stats: {
        ...s.stats,
        count: Long.fromNumber(0),
        first_attempt_count: Long.fromNumber(0),
        max_retries: Long.fromNumber(0),
      },
    }));
    const chartFactory = retryBarChart(withoutRetries);
    return chartFactory(withoutRetries[0]);
  });
