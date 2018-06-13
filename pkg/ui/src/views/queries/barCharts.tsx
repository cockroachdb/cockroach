import d3 from "d3";
import _ from "lodash";
import React from "react";

import { Duration } from "src/util/format";
import { FixLong } from "src/util/fixLong";

const longToInt = d => FixLong(d).toInt();

const countBars = [
  bar("count-first-try", "First Try Count", d => longToInt(d.stats.first_attempt_count)),
  bar("count-retry", "Retry Count", d => longToInt(d.stats.count) - longToInt(d.stats.first_attempt_count)),
];

const rowsBars = [
  bar("rows", "Mean Number of Rows", d => d.stats.num_rows.mean),
];

const latencyBars = [
  bar("latency-parse", "Mean Parse Latency", d => d.stats.parse_lat.mean),
  bar("latency-plan", "Mean Planning Latency", d => d.stats.plan_lat.mean),
  bar("latency-run", "Mean Run Latency", d => d.stats.run_lat.mean),
  bar("latency-overhead", "Mean Overhead Latency", d => d.stats.overhead_lat.mean),
];

function bar(name, title, value) {
  return { name, title, value };
}

function makeBarChart<T, D>(
  accessors: { name: string, value: (T) => D }[],
  formatter: (D) => string = (x: any) => `${x}`,
) {
  return function barChart(rows: T[] = []) {
    function getTotal(d) {
      return _.sum(_.map(accessors, ({ value }) => value(d)));
    }

    const extent = d3.extent(rows, getTotal);

    const scale = d3.scale.linear()
      .domain([0, extent[1]])
      .range([0, 100]);

    return function renderBarChart(d) {
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
export const rowsBarChart = makeBarChart(rowsBars, v => Math.round(v));
export const latencyBarChart = makeBarChart(latencyBars, v => Duration(v * 1e9));
