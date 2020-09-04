// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import d3 from "d3";
import _ from "lodash";
import Long from "long";
import React from "react";
import * as protos from "src/js/protos";
import { stdDevLong } from "src/util/appStats";
import { FixLong } from "src/util/fixLong";
import { Duration } from "src/util/format";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import classNames from "classnames/bind";
import styles from "./barCharts.module.styl";

type StatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type NumericStat = protos.cockroach.sql.INumericStat;

const cx = classNames.bind(styles);

interface BarChartOptions {
  classes?: {
    root?: string;
    label?: string;
  };
}

export const longToInt = (d: number | Long) => Long.fromValue(FixLong(d)).toInt();
const clamp = (i: number) => i < 0 ? 0 : i;

const formatTwoPlaces = d3.format(".2f");

const countBars = [
  bar("count-first-try", (d: StatementStatistics) => longToInt(d.stats.first_attempt_count)),
];

const retryBars = [
  bar("count-retry", (d: StatementStatistics) => longToInt(d.stats.count) - longToInt(d.stats.first_attempt_count)),
];

const rowsBars = [
  bar("rows", (d: StatementStatistics) => d.stats.num_rows.mean),
];

const latencyBars = [
  bar("bar-chart__parse", (d: StatementStatistics) => d.stats.parse_lat.mean),
  bar("bar-chart__plan", (d: StatementStatistics) => d.stats.plan_lat.mean),
  bar("bar-chart__run", (d: StatementStatistics) => d.stats.run_lat.mean),
  bar("bar-chart__overhead", (d: StatementStatistics) => d.stats.overhead_lat.mean),
];

const latencyStdDev = bar(cx("bar-chart__overall-dev"), (d: StatementStatistics) => stdDevLong(d.stats.service_lat, d.stats.count));
const rowsStdDev = bar(cx("rows-dev"), (d: StatementStatistics) => stdDevLong(d.stats.num_rows, d.stats.count));

function bar(name: string, value: (d: StatementStatistics) => number) {
  return { name, value };
}

function renderNumericStatLegend(count: number | Long, stat: number, sd: number, formatter: (d: number) => string) {
  return (
    <table className={cx("numeric-stat-legend")}>
      <tbody>
        <tr>
          <th>
            <div className={cx("numeric-stat-legend__bar", "numeric-stat-legend__bar--mean")} />
            Mean
          </th>
          <td>{ formatter(stat) }</td>
        </tr>
        <tr>
          <th>
            <div className={cx("numeric-stat-legend__bar", "numeric-stat-legend__bar--dev")} />
            Standard Deviation
          </th>
          <td>{ longToInt(count) < 2 ? "-" : sd ? formatter(sd) : "0" }</td>
        </tr>
      </tbody>
    </table>
  );
}

const makeBarChart = (
  type: "grey" | "red",
  accessors: { name: string, value: (d: StatementStatistics) => number }[],
  formatter: (d: number) => string = (x) => `${x}`,
  stdDevAccessor?: { name: string, value: (d: StatementStatistics) => number },
  legendFormatter?: (d: number) => string,
) => {
  if (!legendFormatter) {
    legendFormatter = formatter;
  }

  return (rows: StatementStatistics[] = [], options: BarChartOptions = {}) => {
    const getTotal = (d: StatementStatistics) => _.sum(_.map(accessors, ({ value }) => value(d)));
    const getTotalWithStdDev = (d: StatementStatistics) => getTotal(d) + stdDevAccessor.value(d);

    const extent = d3.extent(rows, stdDevAccessor ? getTotalWithStdDev : getTotal);

    const scale = d3.scale.linear()
      .domain([0, extent[1]])
      .range([0, 100]);

    return (d: StatementStatistics) => {
      if (rows.length === 0) {
        scale.domain([0, getTotal(d)]);
      }

      let sum = 0;
      _.map(accessors, ({ name, value }) => {
        const v = value(d);
        sum += v;
        return (
          <div
            key={ name + v }
            className={cx(name, "bar-chart__bar")}
            style={{ width: scale(v) + "%" }}
          />
        );
      });

      const renderStdDev = () => {
        if (!stdDevAccessor) {
          return null;
        }

        const { name, value } = stdDevAccessor;

        const stddev = value(d);
        const width = stddev + (stddev > sum ? sum : stddev);
        const left = stddev > sum ? 0 : sum - stddev;
        const cn = cx(name, "bar-chart__bar", "bar-chart__bar--dev");
        const style = {
          width: scale(width) + "%",
          left: scale(left) + "%",
        };
        return (
          <div
            className={cn}
            style={style}
          />
        );
      };

      const className = cx("bar-chart", `bar-chart-${type}`, {
        "bar-chart--singleton": rows.length === 0,
        [options?.classes?.root]: !!options?.classes?.root,
      });
      if (stdDevAccessor) {
        const sd = stdDevAccessor.value(d);
        const titleText = renderNumericStatLegend(rows.length, sum, sd, legendFormatter);
        return (
          <div className={ className}>
            <ToolTipWrapper text={ titleText } short>
              <div className={cx("bar-chart__label", options?.classes?.label)}>{ formatter(getTotal(d)) }</div>
              <div className={cx("bar-chart__multiplebars")}>
                <div
                  key="bar-chart__parse"
                  className={cx("bar-chart__parse", "bar-chart__bar")}
                  style={{ width: scale(getTotal(d)) + "%" }}
                />
                { renderStdDev() }
              </div>
            </ToolTipWrapper>
          </div>
        );
      } else {
        return (
          <div className={className}>
            <div className={cx("bar-chart__label", options?.classes?.label)}>{ formatter(getTotal(d)) }</div>
            <div
              key="bar-chart__parse"
              className={cx("bar-chart__parse", "bar-chart__bar")}
              style={{ width: scale(getTotal(d)) + "%" }}
            />
          </div>
        );
      }
    };
  };
};

const SCALE_FACTORS: { factor: number, key: string }[] = [
  { factor: 1000000000, key: "b" },
  { factor: 1000000, key: "m" },
  { factor: 1000, key: "k" },
];

export function approximify(value: number) {
  for (let i = 0; i < SCALE_FACTORS.length; i++) {
    const scale = SCALE_FACTORS[i];
    if (value > scale.factor) {
      return "" + Math.round(value / scale.factor) + scale.key;
    }
  }

  return "" + Math.round(value);
}

export const countBarChart = makeBarChart("grey", countBars, approximify);
export const retryBarChart = makeBarChart("red", retryBars, approximify);
export const rowsBarChart = makeBarChart("grey", rowsBars, approximify, rowsStdDev, formatTwoPlaces);
export const latencyBarChart = makeBarChart("grey", latencyBars, v => Duration(v * 1e9), latencyStdDev);

export function rowsBreakdown(s: StatementStatistics) {
  const mean = s.stats.num_rows.mean;
  const sd = stdDevLong(s.stats.num_rows, s.stats.count);

  const scale = d3.scale.linear()
      .domain([0, mean + sd])
      .range([0, 100]);

  return {
    rowsBarChart(meanRow?: boolean) {
      const spread = scale(sd + (sd > mean ? mean : sd));
      if (meanRow) {
        return formatTwoPlaces(mean);
      } else {
        return spread;
      }
    },
  };
}

export function genericBarChart(s: NumericStat, count: number | Long, format?: (v: number) => string) {
    if (!s) {
      return () => (
        <div/>
      );
    }
    const mean = s.mean;
    const sd = stdDevLong(s, count);

    const max = mean + sd;
    const scale = d3.scale.linear()
      .domain([0, max])
      .range([0, 100]);
    if (!format) {
      format = d3.format(".2f");
    }
    return function MakeGenericBarChart() {
      const width = scale(clamp(mean - sd));
      const right = scale(mean);
      const spread = scale(sd + (sd > mean ? mean : sd));
      const title = renderNumericStatLegend(count, mean, sd, format);
      return (
        <ToolTipWrapper text={ title } short>
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>{ format(mean) }</div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__parse", "bar-chart__bar")}
                style={{ width: right + "%", position: "absolute", left: 0 }}
              />
              <div
                className={cx("bar-chart__parse-dev", "bar-chart__bar", "bar-chart__bar--dev")}
                style={{ width: spread + "%", position: "absolute", left: width + "%" }}
              />
            </div>
          </div>
        </ToolTipWrapper>
      );
    };
}

export function latencyBreakdown(s: StatementStatistics) {
  const parseMean = s.stats.parse_lat.mean;
  const parseSd = stdDevLong(s.stats.parse_lat, s.stats.count);

  const planMean = s.stats.plan_lat.mean;
  const planSd = stdDevLong(s.stats.plan_lat, s.stats.count);

  const runMean = s.stats.run_lat.mean;
  const runSd = stdDevLong(s.stats.run_lat, s.stats.count);

  const overheadMean = s.stats.overhead_lat.mean;
  const overheadSd = stdDevLong(s.stats.overhead_lat, s.stats.count);

  const overallMean = s.stats.service_lat.mean;
  const overallSd = stdDevLong(s.stats.service_lat, s.stats.count);

  const max = Math.max(
    parseMean + parseSd,
    parseMean + planMean + planSd,
    parseMean + planMean + runMean + runSd,
    parseMean + planMean + runMean + overheadMean + overheadSd,
    overallMean + overallSd,
  );

  const format = (v: number) => Duration(v * 1e9);

  const scale = d3.scale.linear()
    .domain([0, max])
    .range([0, 100]);

  return {
    parseBarChart() {
      const width = scale(clamp(parseMean - parseSd));
      const right = scale(parseMean);
      const spread = scale(parseSd + (parseSd > parseMean ? parseMean : parseSd));
      const title = renderNumericStatLegend(s.stats.count, parseMean, parseSd, format);
      return (
        <ToolTipWrapper text={ title } short>
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>{ Duration(parseMean * 1e9) }</div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__parse", "bar-chart__bar")}
                style={{ width: right + "%", position: "absolute", left: 0 }}
              />
              <div
                className={cx("bar-chart__parse-dev", "bar-chart__bar", "bar-chart__bar--dev")}
                style={{ width: spread + "%", position: "absolute", left: width + "%" }}
              />
            </div>
          </div>
        </ToolTipWrapper>
      );
    },

    planBarChart() {
      const left = scale(parseMean);
      const width = scale(clamp(planMean - planSd));
      const right = scale(planMean);
      const spread = scale(planSd + (planSd > planMean ? planMean : planSd));
      const title = renderNumericStatLegend(s.stats.count, planMean, planSd, format);
      return (
        <ToolTipWrapper text={ title } short>
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>{ Duration(planMean * 1e9) }</div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__plan", "bar-chart__bar")}
                style={{ width: right + "%", position: "absolute", left: left + "%" }}
              />
              <div
                className={cx("bar-chart__plan-dev", "bar-chart__bar", "bar-chart__bar--dev")}
                style={{ width: spread + "%", position: "absolute", left: width + left + "%" }}
              />
            </div>
          </div>
        </ToolTipWrapper>
      );
    },

    runBarChart() {
      const left = scale(parseMean + planMean);
      const width = scale(clamp(runMean - runSd));
      const right = scale(runMean);
      const spread = scale(runSd + (runSd > runMean ? runMean : runSd));
      const title = renderNumericStatLegend(s.stats.count, runMean, runSd, format);
      return (
        <ToolTipWrapper text={ title } short>
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>{ Duration(runMean * 1e9) }</div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__run", "bar-chart__bar")}
                style={{ width: right + "%", position: "absolute", left: left + "%" }}
              />
              <div
                className={cx("bar-chart__run-dev", "bar-chart__bar", "bar-chart__bar--dev")}
                style={{ width: spread + "%", position: "absolute", left: width + left + "%" }}
              />
            </div>
          </div>
        </ToolTipWrapper>
      );
    },

    overheadBarChart() {
      const left = scale(parseMean + planMean + runMean);
      const width = scale(clamp(overheadMean - overheadSd));
      const right = scale(overheadMean);
      const spread = scale(overheadSd + (overheadSd > overheadMean ? overheadMean : overheadSd));
      const title = renderNumericStatLegend(s.stats.count, overheadMean, overheadSd, format);
      return (
        <ToolTipWrapper text={ title } short>
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>{ Duration(overheadMean * 1e9) }</div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__overhead", "bar-chart__bar")}
                style={{ width: right + "%", position: "absolute", left: left + "%" }}
              />
              <div
                className={cx("bar-chart__overhead-dev", "bar-chart__bar", "bar-chart__bar--dev")}
                style={{ width: spread + "%", position: "absolute", left: width + left + "%" }}
              />
            </div>
          </div>
        </ToolTipWrapper>
      );
    },

    overallBarChart() {
      const parse = scale(parseMean);
      const plan = scale(planMean);
      const run = scale(runMean);
      const overhead = scale(overheadMean);
      const width = scale(clamp(overallMean - overallSd));
      const spread = scale(overallSd + (overallSd > overallMean ? overallMean : overallSd));
      const title = renderNumericStatLegend(s.stats.count, overallMean, overallSd, format);
      return (
        <ToolTipWrapper text={ title } short>
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>{ Duration(overallMean * 1e9) }</div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__parse", "bar-chart__bar")}
                style={{ width: parse + plan + run + overhead + "%", position: "absolute", left: 0 }}
              />
              <div
                className={cx("bar-chart__overall-dev", "bar-chart__bar", "bar-chart__bar--dev")}
                style={{ width: spread + "%", position: "absolute", left: width + "%" }}
              />
            </div>
          </div>
        </ToolTipWrapper>
      );
    },
  };
}
