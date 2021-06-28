// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { scaleLinear } from "d3-scale";
import { extent as d3Extent } from "d3-array";
import _ from "lodash";
import React from "react";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import styles from "./barCharts.module.scss";
import { NumericStatLegend } from "./numericStatLegend";
import { normalizeClosedDomain } from "./utils";

const cx = classNames.bind(styles);

export interface BarChartOptions<T> {
  classes?: {
    root?: string;
    label?: string;
  };
  displayNoSamples?: (d: T) => boolean;
}

export function barChartFactory<T>(
  type: "grey" | "red",
  accessors: {
    name: string;
    value: (d: T) => number;
  }[],
  formatter: (d: number) => string = x => `${x}`,
  stdDevAccessor?: {
    name: string;
    value: (d: T) => number;
  },
  legendFormatter?: (d: number) => string,
) {
  if (!legendFormatter) {
    legendFormatter = formatter;
  }

  return (rows: T[] = [], options: BarChartOptions<T> = {}) => {
    const getTotal = (d: T) => _.sum(_.map(accessors, ({ value }) => value(d)));
    const getTotalWithStdDev = (d: T) => getTotal(d) + stdDevAccessor.value(d);

    const extent = d3Extent(
      rows,
      stdDevAccessor ? getTotalWithStdDev : getTotal,
    );
    const domain = normalizeClosedDomain([0, extent[1]]);
    const scale = scaleLinear()
      .domain(domain)
      .range([0, 100]);

    return (d: T) => {
      if (rows.length === 0) {
        scale.domain(normalizeClosedDomain([0, getTotal(d)]));
      }

      if (options?.displayNoSamples ? options.displayNoSamples(d) : false) {
        return (
          <Tooltip
            placement="bottom"
            content={
              <div className={cx("tooltip__table--title")}>
                <p>
                  Either the statement sample rate is set to 0, disabling
                  sampling, or statements have not yet been sampled. To turn on
                  sampling, set <code>sql.txn_stats.sample_rate</code> to a
                  nonzero value less than or equal to 1.
                </p>
              </div>
            }
          >
            no samples
          </Tooltip>
        );
      }

      let sum = 0;
      _.map(accessors, ({ name, value }) => {
        const v = value(d);
        sum += v;
        return (
          <div
            key={name + v}
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
        return <div className={cn} style={style} />;
      };

      const className = cx("bar-chart", `bar-chart-${type}`, {
        "bar-chart--singleton": rows.length === 0,
        [options?.classes?.root]: !!options?.classes?.root,
      });
      if (stdDevAccessor) {
        const sd = stdDevAccessor.value(d);
        const titleText = NumericStatLegend(
          rows.length,
          sum,
          sd,
          legendFormatter,
        );
        return (
          <div className={className}>
            <Tooltip content={titleText} style="light">
              <div className={cx("bar-chart__label", options?.classes?.label)}>
                {formatter(getTotal(d))}
              </div>
              <div className={cx("bar-chart__multiplebars")}>
                <div
                  key="bar-chart__parse"
                  className={cx("bar-chart__parse", "bar-chart__bar")}
                  style={{ width: scale(getTotal(d)) + "%" }}
                />
                {renderStdDev()}
              </div>
            </Tooltip>
          </div>
        );
      } else {
        return (
          <div className={className}>
            <div className={cx("bar-chart__label", options?.classes?.label)}>
              {formatter(getTotal(d))}
            </div>
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
}
