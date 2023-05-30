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
import { commonStyles } from "src/common";
import {
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
} from "src/timeScaleDropdown";
import { applyBtn } from "../queryFilter/filterClasses";
import { Menu, Dropdown } from "antd";
import "antd/lib/menu/style";
import "antd/lib/dropdown/style";
import {
  limitOptions,
  limitMoreOptions,
  getSortLabel,
  stmtRequestSortOptions,
  txnRequestSortOptions,
  stmtRequestSortMoreOptions,
  txnRequestSortMoreOptions,
} from "../util/sqlActivityConstants";
import { SqlStatsSortOptions, SqlStatsSortType } from "src/api/statementsApi";
import { CaretDown } from "@cockroachlabs/icons";
import { ClickParam } from "antd/lib/menu";
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

  const changeTop = (event: ClickParam): void => {
    const top = Number(event.key);
    if (top !== topValue) {
      onChangeTop(top);
    }
  };
  const changeBy = (event: ClickParam): void => {
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
