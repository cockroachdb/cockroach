// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect, useState } from "react";
import classNames from "classnames/bind";
import { useHistory } from "react-router-dom";
import {
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable/sortedtable";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Search } from "src/search/search";
import { RecentStatement, RecentStatementFilters } from "src/recentExecutions";
import { Filter } from "src/queryFilter";
import LoadingError from "src/sqlActivity/errorComponent";
import {
  RECENT_STATEMENT_SEARCH_PARAM,
  getAppsFromRecentExecutions,
  filterRecentStatements,
} from "../recentExecutions/recentStatementUtils";
import {
  calculateActiveFilters,
  getFullFiltersAsStringRecord,
} from "../queryFilter/filter";
import { RecentStatementsSection } from "../recentExecutions/recentStatementsSection";
import { inactiveFiltersState } from "../queryFilter/filter";
import { queryByName, syncHistory } from "src/util/query";
import { getTableSortFromURL } from "../sortedtable/getTableSortFromURL";
import { getRecentStatementFiltersFromURL } from "src/queryFilter/utils";

import styles from "./statementsPage.module.scss";

const cx = classNames.bind(styles);

export type RecentStatementsViewDispatchProps = {
  onColumnsSelect: (columns: string[]) => void;
  onFiltersChange: (filters: RecentStatementFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshLiveWorkload: () => void;
};

export type RecentStatementsViewStateProps = {
  selectedColumns: string[];
  statements: RecentStatement[];
  sortSetting: SortSetting;
  sessionsError: Error | null;
  filters: RecentStatementFilters;
  internalAppNamePrefix: string;
  isTenant?: boolean;
};

export type RecentStatementsViewProps = RecentStatementsViewStateProps &
  RecentStatementsViewDispatchProps;

export const RecentStatementsView: React.FC<RecentStatementsViewProps> = ({
  onColumnsSelect,
  refreshLiveWorkload,
  onFiltersChange,
  onSortChange,
  selectedColumns,
  sortSetting,
  statements,
  sessionsError,
  filters,
  internalAppNamePrefix,
  isTenant,
}: RecentStatementsViewProps) => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 20,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, RECENT_STATEMENT_SEARCH_PARAM),
  );

  useEffect(() => {
    // Refresh every 10 seconds.
    refreshLiveWorkload();
    const interval = setInterval(refreshLiveWorkload, 10 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshLiveWorkload]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getRecentStatementFiltersFromURL(history.location);

    if (sortSettingURL) {
      onSortChange(sortSettingURL);
    }

    if (filtersFromURL) {
      onFiltersChange(filtersFromURL);
    }
  }, [history, onSortChange, onFiltersChange]);

  useEffect(() => {
    // This effect runs when the filters or sort settings received from
    // redux changes and syncs the URL params with redux.
    syncHistory(
      {
        ascending: sortSetting.ascending.toString(),
        columnTitle: sortSetting.columnTitle,
        [RECENT_STATEMENT_SEARCH_PARAM]: search,
        ...getFullFiltersAsStringRecord(filters),
      },
      history,
      true,
    );
  }, [
    history,
    filters,
    sortSetting.ascending,
    sortSetting.columnTitle,
    search,
  ]);

  const resetPagination = () => {
    setPagination({
      current: 1,
      pageSize: 20,
    });
  };

  const onSortClick = (ss: SortSetting): void => {
    onSortChange(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string): void => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: RecentStatementFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const clearSearch = () => onSubmitSearch("");
  const clearFilters = () => onSubmitFilters({ app: inactiveFiltersState.app });

  const apps = getAppsFromRecentExecutions(statements, internalAppNamePrefix);
  const countActiveFilters = calculateActiveFilters(filters);

  const filteredStatements = filterRecentStatements(
    statements,
    filters,
    internalAppNamePrefix,
    search,
  );

  return (
    <div className={cx("root")}>
      <PageConfig>
        <PageConfigItem>
          <Search
            onSubmit={onSubmitSearch}
            onClear={clearSearch}
            defaultValue={search}
          />
        </PageConfigItem>
        <PageConfigItem>
          <Filter
            activeFilters={countActiveFilters}
            onSubmitFilters={onSubmitFilters}
            appNames={apps}
            filters={filters}
          />
        </PageConfigItem>
      </PageConfig>
      <div className={cx("table-area")}>
        <Loading
          loading={statements == null}
          page="active statements"
          error={sessionsError}
          renderError={() =>
            LoadingError({
              statsType: "statements",
            })
          }
        >
          <RecentStatementsSection
            filters={filters}
            pagination={pagination}
            search={search}
            statements={filteredStatements}
            selectedColumns={selectedColumns}
            sortSetting={sortSetting}
            onClearFilters={clearFilters}
            onChangeSortSetting={onSortClick}
            onColumnsSelect={onColumnsSelect}
            isTenant={isTenant}
          />
        </Loading>
      </div>
    </div>
  );
};
