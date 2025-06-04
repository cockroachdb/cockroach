// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretDown } from "@cockroachlabs/icons";
import { InlineAlert } from "@cockroachlabs/ui-components";
import { Menu, Dropdown } from "antd";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import { MenuClickEventHandler } from "rc-menu/es/interface";
import React from "react";

import { SqlStatsSortOptions, SqlStatsSortType } from "src/api/statementsApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import {
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  toDateRange,
} from "src/timeScaleDropdown";

import { applyBtn } from "../queryFilter/filterClasses";
import {
  limitOptions,
  limitMoreOptions,
  getSortLabel,
  stmtRequestSortOptions,
  txnRequestSortOptions,
  stmtRequestSortMoreOptions,
  txnRequestSortMoreOptions,
} from "../util/sqlActivityConstants";

import styles from "./searchCriteria.module.scss";

const cx = classNames.bind(styles);
const { SubMenu } = Menu;

type SortOption = {
  label: string;
  value: SqlStatsSortType;
};
export interface SearchCriteriaProps {
  searchType: "Statement" | "Transaction";
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
    searchType,
    topValue,
    byValue,
    currentScale,
    onChangeTop,
    onChangeBy,
    onChangeTimeScale,
  } = props;

  // Check if selected time range is less than 1 hour
  const [start, end] = toDateRange(currentScale);
  const duration = moment.duration(end.diff(start));
  const isSubHourRange = duration.asHours() < 1;
  const sortOptions: SortOption[] =
    searchType === "Statement" ? stmtRequestSortOptions : txnRequestSortOptions;
  const sortMoreOptions: SortOption[] =
    searchType === "Statement"
      ? stmtRequestSortMoreOptions
      : txnRequestSortMoreOptions;

  const warning = (
    <span className={cx("options-warning", "large")}>
      You may experience a longer loading time when selecting options below.
    </span>
  );

  const changeTop: MenuClickEventHandler = event => {
    const top = Number(event.key);
    if (top !== topValue) {
      onChangeTop(top);
    }
  };
  const changeBy: MenuClickEventHandler = event => {
    const by = Object.values(SqlStatsSortOptions).find(
      s => s === Number(event.key),
    );
    if (by !== byValue) {
      onChangeBy(by as SqlStatsSortType);
    }
  };

  const menuTop = (
    <Menu onClick={changeTop}>
      {limitOptions.map(option => (
        <Menu.Item key={option.value}>{option.label}</Menu.Item>
      ))}
      <SubMenu title="More">
        {warning}
        {limitMoreOptions.map(option => (
          <Menu.Item key={option.value}>{option.label}</Menu.Item>
        ))}
      </SubMenu>
    </Menu>
  );

  const menuBy = (
    <Menu onClick={changeBy}>
      {sortOptions.map(option => (
        <Menu.Item key={option.value}>{option.label}</Menu.Item>
      ))}
      <SubMenu title="More">
        {warning}
        {sortMoreOptions.map(option => (
          <Menu.Item key={option.value}>{option.label}</Menu.Item>
        ))}
      </SubMenu>
    </Menu>
  );

  return (
    <div className={cx("search-area")}>
      <h5 className={commonStyles("base-heading")}>Search Criteria</h5>
      {isSubHourRange && (
        <div style={{ marginBottom: "16px" }}>
          <InlineAlert
            intent="warning"
            title="Sub-hour time range selected: Statement statistics are aggregated hourly by default. You may not see data for time ranges less than 1 hour if the aggregation interval has not elapsed yet. Adjust the sql.stats.aggregation.interval cluster setting if you need finer granularity."
          />
        </div>
      )}
      <PageConfig className={cx("top-area")}>
        <PageConfigItem>
          <label>
            <span className={cx("label")}>Top</span>
            <Dropdown overlay={menuTop} trigger={["click"]}>
              <div className={cx("dropdown-area", "small")}>
                <div className={cx("dropdown-value-small")}>{topValue}</div>
                <CaretDown className={cx("arrow-down")} />
              </div>
            </Dropdown>
          </label>
        </PageConfigItem>
        <PageConfigItem>
          <label>
            <span className={cx("label")}>By</span>
            <Dropdown overlay={menuBy} trigger={["click"]}>
              <div className={cx("dropdown-area", "medium")}>
                <div className={cx("dropdown-value-medium")}>
                  {getSortLabel(byValue, searchType)}
                </div>
                <CaretDown className={cx("arrow-down")} />
              </div>
            </Dropdown>
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
