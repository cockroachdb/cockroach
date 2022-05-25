// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { CSSProperties } from "react";
import { Link } from "react-router-dom";
import { Loading } from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./keyVizualizer.module.styl";
import ErrorBoundary from "../app/components/errorMessage/errorBoundary";
import { CellInfoDetails } from "src/redux/keyVizualizer/keyVizualizerReducer";
import { FilterIcon } from "src/components/icon/filter";
import { BytesToUnitValue, RawToNormalizedValue } from "src/util/format";
import { round } from "lodash";

const cx = classNames.bind(styles);

export interface KeyVizualizerTooltipProps {
  x: number;
  y: number;
  applyFilter: (filterType: string, id: string | number) => void;
  cellDetails: CellInfoDetails;
  error: Error;
  loading: boolean;
}
export const KeyVizualizerTooltip = (props: KeyVizualizerTooltipProps) => {
  const { x, y, applyFilter, cellDetails, error, loading } = props;
  const style = { "--x": `${x + 60}px`, "--y": `${y + 30}px` } as CSSProperties;

  const formatMetric = (
    value: number,
    isBytes: boolean,
    roundAmount: number,
  ): string => {
    const result = isBytes
      ? BytesToUnitValue(value)
      : RawToNormalizedValue(value);
    return round(result.value, roundAmount).toString() + result.units;
  };

  return (
    <div className={cx("span-tooltip")} style={style}>
      <ErrorBoundary>
        <Loading
          loading={loading}
          error={error}
          page={undefined}
          render={() => (
            <div>
              <p>
                Range:{" "}
                <Link to={`/reports/range/${cellDetails?.rangeId}`}>
                  {cellDetails?.rangeId}
                </Link>
              </p>
              <p>QPS: {formatMetric(cellDetails?.qps, false, 2)}</p>
              <p>Bytes: {formatMetric(cellDetails?.keyBytes, true, 1)}</p>
              <p>Leaseholder: {cellDetails?.leaseholder}</p>
              <p>
                Nodes:{" "}
                {cellDetails?.nodes.map(
                  (nodeId: number, idx: number, arr: string | any[]) => (
                    <Link to={`/node/${nodeId}`}>
                      {nodeId}
                      {idx < arr.length - 1 && ", "}
                    </Link>
                  ),
                )}{" "}
                | {/* which node do we apply filter here for? */}
                {
                  <span
                    onClick={() => applyFilter("node", cellDetails?.nodes[0])}
                  >
                    <FilterIcon
                      fill={"#1890FF"}
                      className={cx("filter-icon")}
                    />
                  </span>
                }
              </p>
              <p>
                Store: {cellDetails?.store} |{" "}
                {
                  <span
                    onClick={() => applyFilter("store", cellDetails?.store)}
                  >
                    <FilterIcon
                      fill={"#1890FF"}
                      className={cx("filter-icon")}
                    />
                  </span>
                }
              </p>
              <p>
                Database:{" "}
                <Link to={`/database/${cellDetails?.schema?.database}`}>
                  {cellDetails?.schema?.database}
                </Link>{" "}
                |{" "}
                {
                  <span
                    onClick={() =>
                      applyFilter("database", cellDetails?.schema?.database)
                    }
                  >
                    <FilterIcon
                      fill={"#1890FF"}
                      className={cx("filter-icon")}
                    />
                  </span>
                }
              </p>
              <p>
                Table:{" "}
                <Link
                  to={`/database/${cellDetails?.schema?.database}/table/${cellDetails?.schema?.table}`}
                >
                  {cellDetails?.schema?.table}
                </Link>{" "}
                |{" "}
                {
                  <span
                    onClick={() =>
                      applyFilter("table", cellDetails?.schema?.table)
                    }
                  >
                    <FilterIcon
                      fill={"#1890FF"}
                      className={cx("filter-icon")}
                    />
                  </span>
                }
              </p>
              <p>
                Index: {cellDetails?.index} |{" "}
                {
                  <span
                    onClick={() => applyFilter("index", cellDetails?.index)}
                  >
                    <FilterIcon
                      fill={"#1890FF"}
                      className={cx("filter-icon")}
                    />
                  </span>
                }
              </p>
            </div>
          )}
        />
      </ErrorBoundary>
    </div>
  );
};
