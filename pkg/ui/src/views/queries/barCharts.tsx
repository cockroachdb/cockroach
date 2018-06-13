import d3 from "d3";
import _ from "lodash";
import React from "react";

import { Duration } from "src/util/format";
import { FixLong } from "src/util/fixLong";

import * as protos from "src/js/protos";

type StatementStatistics = protos.cockroach.sql.CollectedStatementStatistics$Properties;

const longToInt = (d: number | Long) => FixLong(d).toInt();

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
            title={ title + ": " + formatter(v) }
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

export const countBarChart = makeBarChart(countBars);
export const rowsBarChart = makeBarChart(rowsBars, v => "" + Math.round(v));
export const latencyBarChart = makeBarChart(latencyBars, v => Duration(v * 1e9));
