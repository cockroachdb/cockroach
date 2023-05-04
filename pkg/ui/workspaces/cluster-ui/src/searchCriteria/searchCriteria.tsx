// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import styles from "./searchCriteria.module.scss";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Button } from "src/button";
import { commonStyles, selectCustomStyles } from "src/common";
import {
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
} from "src/timeScaleDropdown";
import { applyBtn } from "../queryFilter/filterClasses";
import Select from "react-select";
import { limitOptions } from "../util/sqlActivityConstants";
import { SqlStatsSortType } from "src/api/statementsApi";
const cx = classNames.bind(styles);

type SortOption = {
  label: string;
  value: SqlStatsSortType;
};
export interface SearchCriteriaProps {
  sortOptions: SortOption[];
  currentScale: TimeScale;
  topValue: number;
  byValue: SqlStatsSortType;
  onChangeTimeScale: (ts: TimeScale) => void;
  onChangeTop: (top: number) => void;
  onChangeBy: (by: SqlStatsSortType) => void;
  onApply: () => void;
}

export function SearchCriteria(props: SearchCriteriaProps): React.ReactElement {
  const {
    topValue,
    byValue,
    currentScale,
    onChangeTop,
    onChangeBy,
    onChangeTimeScale,
    sortOptions,
  } = props;
  const customStyles = { ...selectCustomStyles };
  customStyles.indicatorSeparator = (provided: any) => ({
    ...provided,
    display: "none",
  });

  const customStylesTop = { ...customStyles };
  customStylesTop.container = (provided: any) => ({
    ...provided,
    width: "80px",
    border: "none",
    lineHeight: "29px",
  });

  const customStylesBy = { ...customStyles };
  customStylesBy.container = (provided: any) => ({
    ...provided,
    width: "170px",
    border: "none",
    lineHeight: "29px",
  });

  return (
    <div className={cx("search-area")}>
      <h5 className={commonStyles("base-heading")}>Search Criteria</h5>
      <PageConfig className={cx("top-area")}>
        <PageConfigItem>
          <label>
            <span className={cx("label")}>Top</span>
            <Select
              options={limitOptions}
              value={limitOptions.filter(top => top.value === topValue)}
              onChange={event => onChangeTop(event.value)}
              styles={customStylesTop}
            />
          </label>
        </PageConfigItem>
        <PageConfigItem>
          <label>
            <span className={cx("label")}>By</span>
            <Select
              options={sortOptions}
              value={sortOptions.filter(
                (top: SortOption) => top.value === byValue,
              )}
              onChange={event => onChangeBy(event.value as SqlStatsSortType)}
              styles={customStylesBy}
            />
          </label>
        </PageConfigItem>
        <PageConfigItem>
          <label>
            <span className={cx("label")}>Time Range</span>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={currentScale}
              setTimeScale={onChangeTimeScale}
              className={cx("timescale-small")}
            />
          </label>
        </PageConfigItem>
        <PageConfigItem>
          <Button
            className={`${applyBtn.btn} ${cx("margin-top-btn")}`}
            textAlign="center"
            onClick={props.onApply}
          >
            Apply
          </Button>
        </PageConfigItem>
      </PageConfig>
    </div>
  );
}
