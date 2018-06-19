import d3 from "d3";
import _ from "lodash";
import React from "react";

import { stdDevLong } from "src/util/appStats";
import { Duration } from "src/util/format";
import { FixLong } from "src/util/fixLong";

import * as protos from "src/js/protos";

type StatementStatistics = protos.cockroach.sql.CollectedStatementStatistics$Properties;

const longToInt = (d: number | Long) => FixLong(d).toInt();
const clamp = (i: number) => i < 0 ? 0 : i;

const countBars = [
  bar("count-first-try", "First Try Count", (d: StatementStatistics) => longToInt(d.stats.first_attempt_count)),
  bar("count-retry", "Retry Count", (d: StatementStatistics) => longToInt(d.stats.count) - longToInt(d.stats.first_attempt_count)),
];

const rowsBars = [
  bar("rows", "Mean Number of Rows", (d: StatementStatistics) => d.stats.num_rows.mean),
];

const latencyBars = [
  bar("latency-parse", "Mean Parse Latency", (d: StatementStatistics) => d.stats.parse_lat.mean),
  bar("latency-plan", "Mean Planning Latency", (d: StatementStatistics) => d.stats.plan_lat.mean),
  bar("latency-run", "Mean Run Latency", (d: StatementStatistics) => d.stats.run_lat.mean),
  bar("latency-overhead", "Mean Overhead Latency", (d: StatementStatistics) => d.stats.overhead_lat.mean),
];

function bar(name: string, title: string, value: (d: StatementStatistics) => number) {
  return { name, title, value };
}

function makeBarChart(
  accessors: { name: string, title: string, value: (d: StatementStatistics) => number }[],
  formatter: (d: number) => string = (x) => `${x}`,
) {
  return function barChart(rows: StatementStatistics[] = []) {
    function getTotal(d: StatementStatistics) {
      return _.sum(_.map(accessors, ({ value }) => value(d)));
    }

    const extent = d3.extent(rows, getTotal);

    const scale = d3.scale.linear()
      .domain([0, extent[1]])
      .range([0, 100]);

    return function renderBarChart(d: StatementStatistics) {
      if (rows.length === 0) {
        scale.domain([0, getTotal(d)]);
      }

      const bars = accessors.map(({ name, title, value }) => {
        const v = value(d);
        return (
          <div
            key={ name + v }
            className={ name + " bar-chart__bar" }
            style={{ width: scale(v) + "%" }}
            title={ title + ": " + v }
          />
        );
      });

      return (
        <div className={ "bar-chart" + (rows.length === 0 ? " bar-chart--singleton" : "") }>
          <div className="label">{ formatter(getTotal(d)) }</div>
          { bars }
        </div>
      );
    };
  };
}

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

export const countBarChart = makeBarChart(countBars, approximify);
export const rowsBarChart = makeBarChart(rowsBars, approximify);
export const latencyBarChart = makeBarChart(latencyBars, v => Duration(v * 1e9));

export function countBreakdown(s: StatementStatistics) {
  const count = longToInt(s.stats.count);
  const firstAttempts = longToInt(s.stats.first_attempt_count);
  const retries = count - firstAttempts;
  const maxRetries = longToInt(s.stats.max_retries);

  const scale = d3.scale.linear()
    .domain([0, count])
    .range([0, 100]);

  return {
    firstAttemptsBarChart() {
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="count-first-try bar-chart__bar"
            style={{ width: scale(firstAttempts) + "%" }}
            title={ "First Try Count: " + firstAttempts }
          />
        </div>
      );
    },

    retriesBarChart() {
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="count-retry bar-chart__bar"
            style={{ width: scale(retries) + "%", position: "absolute", right: "0" }}
            title={ "Retry Count: " + retries }
          />
        </div>
      );
    },

    maxRetriesBarChart() {
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="count-retry bar-chart__bar"
            style={{ width: scale(maxRetries) + "%", position: "absolute", right: "0" }}
            title={ "Max Retries: " + retries }
          />
        </div>
      );
    },
  };
}

export function rowsBreakdown(s: StatementStatistics) {
  const mean = s.stats.num_rows.mean;
  const sd = stdDevLong(s.stats.num_rows, s.stats.count);

  const scale = d3.scale.linear()
      .domain([0, mean + sd])
      .range([0, 100]);

  return {
    rowsBarChart() {
      const width = scale(clamp(mean - sd));
      const right = scale(mean);
      const spread = scale(sd + (sd > mean ? mean : sd));
      const title = "Row Count.  Mean: " + mean + " Std.Dev.: " + sd;
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="rows bar-chart__bar"
            style={{ width: width + "%" }}
            title={ title }
          />
          <div
            className="rows-dev bar-chart__bar"
            style={{ width: spread + "%" }}
            title={ title }
          />
          <div
            className="rows bar-chart__bar"
            style={{ width: 1, position: "absolute", left: right + "%" }}
            title={ title }
          />
        </div>
      );
    },
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

  const scale = d3.scale.linear()
    .domain([0, max])
    .range([0, 100]);

  return {
    parseBarChart() {
      const width = scale(clamp(parseMean - parseSd));
      const right = scale(parseMean);
      const spread = scale(parseSd + (parseSd > parseMean ? parseMean : parseSd));
      const title = "Parse Latency.  Mean: " + parseMean + " Std. Dev.: " + parseSd;
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="latency-parse bar-chart__bar"
            style={{ width: width + "%", position: "absolute", left: 0 }}
            title={ title }
          />
          <div
            className="latency-parse-dev bar-chart__bar"
            style={{ width: spread + "%", position: "absolute", left: width + "%" }}
            title={ title }
          />
          <div
            className="latency-parse bar-chart__bar"
            style={{ width: 1, position: "absolute", left: right + "%" }}
            title={ title }
          />
        </div>
      );
    },

    planBarChart() {
      const left = scale(parseMean);
      const width = scale(clamp(planMean - planSd));
      const right = scale(parseMean + planMean);
      const spread = scale(planSd + (planSd > planMean ? planMean : planSd));
      const title = "Plan Latency.  Mean: " + planMean + " Std. Dev.: " + planSd;
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="latency-plan bar-chart__bar"
            style={{ width: width + "%", position: "absolute", left: left + "%" }}
            title={ title }
          />
          <div
            className="latency-plan-dev bar-chart__bar"
            style={{ width: spread + "%", position: "absolute", left: width + left + "%" }}
            title={ title }
          />
          <div
            className="latency-plan bar-chart__bar"
            style={{ width: 1, position: "absolute", left: right + "%" }}
            title={ title }
          />
        </div>
      );
    },

    runBarChart() {
      const left = scale(parseMean + planMean);
      const width = scale(clamp(runMean - runSd));
      const right = scale(parseMean + planMean + runMean);
      const spread = scale(runSd + (runSd > runMean ? runMean : runSd));
      const title = "Run Latency.  Mean: " + runMean + " Std. Dev.: " + runSd;
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="latency-run bar-chart__bar"
            style={{ width: width + "%", position: "absolute", left: left + "%" }}
            title={ title }
          />
          <div
            className="latency-run-dev bar-chart__bar"
            style={{ width: spread + "%", position: "absolute", left: width + left + "%" }}
            title={ title }
          />
          <div
            className="latency-run bar-chart__bar"
            style={{ width: 1, position: "absolute", left: right + "%" }}
            title={ title }
          />
        </div>
      );
    },

    overheadBarChart() {
      const left = scale(parseMean + planMean + runMean);
      const width = scale(clamp(overheadMean - overheadSd));
      const right = scale(parseMean + planMean + runMean + overheadMean);
      const spread = scale(overheadSd + (overheadSd > overheadMean ? overheadMean : overheadSd));
      const title = "Overhead Latency.  Mean: " + overheadMean + " Std. Dev.: " + overheadSd;
      return (
        <div className="bar-chart bar-chart--breakdown">
          <div
            className="latency-overhead bar-chart__bar"
            style={{ width: width + "%", position: "absolute", left: left + "%" }}
            title={ title }
          />
          <div
            className="latency-overhead-dev bar-chart__bar"
            style={{ width: spread + "%", position: "absolute", left: width + left + "%" }}
            title={ title }
          />
          <div
            className="latency-overhead bar-chart__bar"
            style={{ width: 1, position: "absolute", left: right + "%" }}
            title={ title }
          />
        </div>
      );
    },

    overallBarChart() {
      return (
        <div>Unimplemented.</div>
      );
    },
  };
}
