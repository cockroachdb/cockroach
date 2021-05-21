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

export interface BarChartOptions {
  classes?: {
    root?: string;
    label?: string;
  };
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

  return (rows: T[] = [], options: BarChartOptions = {}) => {
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
