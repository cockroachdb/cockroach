// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { CSSProperties, useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Link } from "react-router-dom";
import { Loading } from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./keyVizualizer.module.styl";
import ErrorBoundary from "../app/components/errorMessage/errorBoundary";
import {
  selectCellInfoSlice,
  selectCellInfoByKey,
} from "src/redux/keyVizualizer/keyVizualizerSelectors";
import { actions } from "src/redux/keyVizualizer/keyVizualizerReducer";
import { FilterIcon } from "src/components/icon/filter";
import { BytesToUnitValue, RawToNormalizedValue } from "src/util/format";
import { round } from "lodash";

const cx = classNames.bind(styles);

export interface KeyVizualizerTooltipProps {
  x: number;
  y: number;
  startKey: string;
  endKey: string;
  applyFilter: (filterType: string, id: string | number) => void;
}
export const KeyVizualizerTooltip = (props: KeyVizualizerTooltipProps) => {
  const { x, y, startKey, endKey, applyFilter } = props;
  const dispatch = useDispatch();
  const { error, loading } = useSelector(selectCellInfoSlice);
  const currentCellInfo = useSelector(selectCellInfoByKey(startKey, endKey));
  const style = { "--x": `${x + 60}px`, "--y": `${y + 30}px` } as CSSProperties;
  useEffect(() => {
    if (!currentCellInfo) {
      dispatch(actions.requestCellInfo({ startKey, endKey }));
    }
  }, [dispatch, currentCellInfo, startKey, endKey]);

  const formatMetric = (value: number, isBytes: boolean, roundAmount: number): string => {
    const result = isBytes ? BytesToUnitValue(value) : RawToNormalizedValue(value);
    return round(result.value, roundAmount).toString() + result.units;
  }

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
                <Link to={`/reports/range/${currentCellInfo?.rangeId}`}>
                  {currentCellInfo?.rangeId}
                </Link>
              </p>
              <p>QPS: {formatMetric(currentCellInfo?.qps, false, 2)}</p>
              <p>Bytes: {formatMetric(currentCellInfo?.keyBytes, true, 1)}</p>
              <p>Leaseholder: {currentCellInfo?.leaseholder}</p>
              <p>
                Nodes:{" "}
                {currentCellInfo?.nodes.map(
                  (nodeId: number, idx: number, arr: string | any[]) => (
                    <Link to={`/node/${nodeId}`}>
                      {nodeId}
                      {idx < arr.length - 1 && ", "}
                    </Link>
                  ),
                )}{" "}
                |{" "}
                {/* which node do we apply filter here for? */}
                {<span onClick={() => applyFilter("node", currentCellInfo?.nodes[0])}><FilterIcon fill={"#1890FF"} className={cx("filter-icon")} /></span>}
              </p>
              <p>
                Store: {currentCellInfo?.store} |{" "}
                {<span onClick={() => applyFilter("store", currentCellInfo?.store)}><FilterIcon fill={"#1890FF"} className={cx("filter-icon")} /></span>}
              </p>
              <p>
                Database:{" "}
                <Link to={`/database/${currentCellInfo?.schema?.database}`}>
                  {currentCellInfo?.schema?.database}
                </Link>{" "}
                |{" "}
                {<span onClick={() => applyFilter("database", currentCellInfo?.schema?.database)}><FilterIcon fill={"#1890FF"} className={cx("filter-icon")} /></span>}
              </p>
              <p>
                Table:{" "}
                <Link
                  to={`/database/${currentCellInfo?.schema?.database}/table/${currentCellInfo?.schema?.table}`}
                >
                  {currentCellInfo?.schema?.table}
                </Link>{" "}
                |{" "}
                {<span onClick={() => applyFilter("table", currentCellInfo?.schema?.table)}><FilterIcon fill={"#1890FF"} className={cx("filter-icon")} /></span>}
              </p>
              <p>
                Index: {currentCellInfo?.index} |{" "}
                {<span onClick={() => applyFilter("index", currentCellInfo?.index)}><FilterIcon fill={"#1890FF"} className={cx("filter-icon")} /></span>}
              </p>
            </div>
          )}
        />
      </ErrorBoundary>
    </div>
  );
};
