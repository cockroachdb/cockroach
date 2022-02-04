// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";
import React from "react";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Search } from "src/search";
import { StatisticType } from "src/statsTableUtil/statsTableUtil";
import { syncHistory } from "src/util/query";
import { commonStyles } from "../common";
import { calculateActiveFilters, Filter, Filters } from "../queryFilter";
import ClearStats from "../sqlActivity/clearStats";
import {
  defaultTimeScaleSelected,
  TimeScale,
  TimeScaleDropdown,
} from "../timeScaleDropdown";
import styles from "./statementsPage.module.scss";
import { useHistory } from "react-router-dom";

const cx = classNames.bind(styles);

type SQLStatsPageSettingsProps = {
  apps: string[];
  databases?: string[];
  filters: Filters;
  nodes: number[];
  regions: string[];
  searchInit: string;
  statType: StatisticType;
  timeScale: TimeScale;

  onChangeTimeScale: (ts: TimeScale) => void;
  onSearchSubmit: (search: string) => void;
  onStatementClick?: (statement: string) => void;
  onSubmitFilters: (filters: Filters) => void;
  resetSQLStats: () => void;
};

const SQLStatsPageSettings = ({
  apps,
  databases,
  filters,
  nodes,
  statType,
  regions,
  searchInit,
  timeScale,
  onChangeTimeScale,
  onSearchSubmit,
  onSubmitFilters,
  resetSQLStats,
}: SQLStatsPageSettingsProps): React.ReactElement => {
  const history = useHistory();

  const onSearch = (search: string): void => {
    onSearchSubmit(search);
    syncHistory(
      {
        q: search === "" ? undefined : search,
      },
      history,
    );
  };

  const activeFilters = calculateActiveFilters(filters);
  const resetTime = () => {
    onChangeTimeScale(defaultTimeScaleSelected);
  };

  return (
    <PageConfig>
      <PageConfigItem>
        <Search
          onSubmit={onSearch}
          onClear={() => {
            onSearch("");
          }}
          defaultValue={searchInit}
        />
      </PageConfigItem>
      <PageConfigItem>
        <Filter
          onSubmitFilters={onSubmitFilters}
          appNames={apps}
          dbNames={databases}
          regions={regions}
          nodes={nodes.map((n: number) => "n" + n)}
          activeFilters={activeFilters}
          filters={filters}
          showSqlType={true}
          showScan={true}
          showRegions={regions.length > 1}
          showNodes={nodes.length > 1}
        />
      </PageConfigItem>
      <PageConfigItem className={commonStyles("separator")}>
        <TimeScaleDropdown
          currentScale={timeScale}
          setTimeScale={onChangeTimeScale}
        />
      </PageConfigItem>
      <PageConfigItem>
        <button className={cx("reset-btn")} onClick={resetTime}>
          reset time
        </button>
      </PageConfigItem>
      <PageConfigItem className={commonStyles("separator")}>
        <ClearStats resetSQLStats={resetSQLStats} tooltipType={statType} />
      </PageConfigItem>
    </PageConfig>
  );
};

export default SQLStatsPageSettings;
