// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import { scaleLinear } from "d3-scale";
import { format as d3Format } from "d3-format";

import { stdDevLong, longToInt, NumericStat } from "src/util";
import { Tooltip } from "@cockroachlabs/ui-components";
import { clamp, normalizeClosedDomain } from "./utils";
import styles from "./barCharts.module.scss";

const cx = classNames.bind(styles);

function renderNumericStatLegend(
  count: number | Long,
  stat: number,
  sd: number,
  formatter: (d: number) => string,
) {
  return (
    <table className={cx("numeric-stat-legend")}>
      <tbody>
        <tr>
          <th>
            <div
              className={cx(
                "numeric-stat-legend__bar",
                "numeric-stat-legend__bar--mean",
              )}
            />
            Mean
          </th>
          <td>{formatter(stat)}</td>
        </tr>
        <tr>
          <th>
            <div
              className={cx(
                "numeric-stat-legend__bar",
                "numeric-stat-legend__bar--dev",
              )}
            />
            Standard Deviation
          </th>
          <td>{longToInt(count) < 2 ? "-" : sd ? formatter(sd) : "0"}</td>
        </tr>
      </tbody>
    </table>
  );
}

export function genericBarChart(
  s: NumericStat,
  count: number | Long,
  format?: (v: number) => string,
) {
  if (!s) {
    return () => <div />;
  }
  const mean = s.mean;
  const sd = stdDevLong(s, count);

  const max = mean + sd;
  const scale = scaleLinear()
    .domain(normalizeClosedDomain([0, max]))
    .range([0, 100]);
  if (!format) {
    format = d3Format(".2f");
  }
  return function MakeGenericBarChart() {
    const width = scale(clamp(mean - sd));
    const right = scale(mean);
    const spread = scale(sd + (sd > mean ? mean : sd));
    const title = renderNumericStatLegend(count, mean, sd, format);
    return (
      <Tooltip content={title} style="light">
        <div className={cx("bar-chart", "bar-chart--breakdown")}>
          <div className={cx("bar-chart__label")}>{format(mean)}</div>
          <div className={cx("bar-chart__multiplebars")}>
            <div
              className={cx("bar-chart__parse", "bar-chart__bar")}
              style={{ width: right + "%", position: "absolute", left: 0 }}
            />
            <div
              className={cx(
                "bar-chart__parse-dev",
                "bar-chart__bar",
                "bar-chart__bar--dev",
              )}
              style={{
                width: spread + "%",
                position: "absolute",
                left: width + "%",
              }}
            />
          </div>
        </div>
      </Tooltip>
    );
  };
}
