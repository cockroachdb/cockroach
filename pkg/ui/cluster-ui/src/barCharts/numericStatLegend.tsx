import React from "react";
import classNames from "classnames/bind";
import styles from "./barCharts.module.scss";
import { longToInt } from "../util";

const cx = classNames.bind(styles);

export function NumericStatLegend(
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
