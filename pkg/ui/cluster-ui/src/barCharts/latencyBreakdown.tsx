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
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { stdDevLong } from "src/util";
import { NumericStatLegend } from "./numericStatLegend";
import { scaleLinear } from "d3-scale";
import { Duration } from "src/util/format";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import styles from "./barCharts.module.scss";
import { clamp, normalizeClosedDomain } from "./utils";

type StatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
const cx = classNames.bind(styles);

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
  const domain = normalizeClosedDomain([0, max]);

  const scale = scaleLinear()
    .domain(domain)
    .range([0, 100]);

  return {
    parseBarChart() {
      const width = scale(clamp(parseMean - parseSd));
      const right = scale(parseMean);
      const spread = scale(
        parseSd + (parseSd > parseMean ? parseMean : parseSd),
      );
      const title = NumericStatLegend(
        s.stats.count,
        parseMean,
        parseSd,
        format,
      );
      return (
        <Tooltip content={title} style="light">
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>
              {Duration(parseMean * 1e9)}
            </div>
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
    },

    planBarChart() {
      const left = scale(parseMean);
      const width = scale(clamp(planMean - planSd));
      const right = scale(planMean);
      const spread = scale(planSd + (planSd > planMean ? planMean : planSd));
      const title = NumericStatLegend(s.stats.count, planMean, planSd, format);
      return (
        <Tooltip content={title} style="light">
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>
              {Duration(planMean * 1e9)}
            </div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__plan", "bar-chart__bar")}
                style={{
                  width: right + "%",
                  position: "absolute",
                  left: left + "%",
                }}
              />
              <div
                className={cx(
                  "bar-chart__plan-dev",
                  "bar-chart__bar",
                  "bar-chart__bar--dev",
                )}
                style={{
                  width: spread + "%",
                  position: "absolute",
                  left: width + left + "%",
                }}
              />
            </div>
          </div>
        </Tooltip>
      );
    },

    runBarChart() {
      const left = scale(parseMean + planMean);
      const width = scale(clamp(runMean - runSd));
      const right = scale(runMean);
      const spread = scale(runSd + (runSd > runMean ? runMean : runSd));
      const title = NumericStatLegend(s.stats.count, runMean, runSd, format);
      return (
        <Tooltip content={title} style="light">
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>
              {Duration(runMean * 1e9)}
            </div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__run", "bar-chart__bar")}
                style={{
                  width: right + "%",
                  position: "absolute",
                  left: left + "%",
                }}
              />
              <div
                className={cx(
                  "bar-chart__run-dev",
                  "bar-chart__bar",
                  "bar-chart__bar--dev",
                )}
                style={{
                  width: spread + "%",
                  position: "absolute",
                  left: width + left + "%",
                }}
              />
            </div>
          </div>
        </Tooltip>
      );
    },

    overheadBarChart() {
      const left = scale(parseMean + planMean + runMean);
      const width = scale(clamp(overheadMean - overheadSd));
      const right = scale(overheadMean);
      const spread = scale(
        overheadSd + (overheadSd > overheadMean ? overheadMean : overheadSd),
      );
      const title = NumericStatLegend(
        s.stats.count,
        overheadMean,
        overheadSd,
        format,
      );
      return (
        <Tooltip content={title} style="light">
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>
              {Duration(overheadMean * 1e9)}
            </div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__overhead", "bar-chart__bar")}
                style={{
                  width: right + "%",
                  position: "absolute",
                  left: left + "%",
                }}
              />
              <div
                className={cx(
                  "bar-chart__overhead-dev",
                  "bar-chart__bar",
                  "bar-chart__bar--dev",
                )}
                style={{
                  width: spread + "%",
                  position: "absolute",
                  left: width + left + "%",
                }}
              />
            </div>
          </div>
        </Tooltip>
      );
    },

    overallBarChart() {
      const parse = scale(parseMean);
      const plan = scale(planMean);
      const run = scale(runMean);
      const overhead = scale(overheadMean);
      const width = scale(clamp(overallMean - overallSd));
      const spread = scale(
        overallSd + (overallSd > overallMean ? overallMean : overallSd),
      );
      const title = NumericStatLegend(
        s.stats.count,
        overallMean,
        overallSd,
        format,
      );
      return (
        <Tooltip content={title} style="light">
          <div className={cx("bar-chart", "bar-chart--breakdown")}>
            <div className={cx("bar-chart__label")}>
              {Duration(overallMean * 1e9)}
            </div>
            <div className={cx("bar-chart__multiplebars")}>
              <div
                className={cx("bar-chart__parse", "bar-chart__bar")}
                style={{
                  width: parse + plan + run + overhead + "%",
                  position: "absolute",
                  left: 0,
                }}
              />
              <div
                className={cx(
                  "bar-chart__overall-dev",
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
    },
  };
}
