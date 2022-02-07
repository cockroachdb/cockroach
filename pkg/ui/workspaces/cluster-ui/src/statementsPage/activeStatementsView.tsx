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
  Loading,
  PageConfig,
  PageConfigItem,
  Search,
  SortSetting,
} from "src";
import { ActiveStatement, ActiveStatementFilters } from "src/activeExecutions";
import { defaultFilters, Filter } from "src/queryFilter";
import SQLActivityError from "src/sqlActivity/errorComponent";
import {
  ACTIVE_STATEMENT_SEARCH_PARAM,
  appsFromActiveStatements,
} from "../activeExecutions/activeStatementUtils";
import { useQueryParmeters } from "../hooks/useQueryParameters";
import { calculateActiveFilters } from "../queryFilter/filter";
import { useFiltersFromURL } from "../queryFilter/useFiltersFromURL";

import styles from "./statementsPage.module.scss";
import { ActiveStatementsSection } from "../activeExecutions/activeStatementsSection";
import { inactiveFiltersState } from "../queryFilter/filter";
const cx = classNames.bind(styles);

export type ActiveStatementsViewDispatchProps = {
  onColumnsSelect: (columns: string[]) => void;
  refreshSessions: () => void;
};

export type ActiveStatementsViewStateProps = {
  selectedColumns: string[];
  statements: ActiveStatement[];
  fetchError: Error | null;
};

export type ActiveStatementsViewProps = ActiveStatementsViewStateProps &
  ActiveStatementsViewDispatchProps;

export const ActiveStatementsView: React.FC<ActiveStatementsViewProps> = ({
  onColumnsSelect,
  refreshSessions,
  selectedColumns,
  statements,
  fetchError,
}: ActiveStatementsViewProps) => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 20,
  });
  const history = useHistory();
  const queryParams = useQueryParmeters();
  const search = queryParams.get(ACTIVE_STATEMENT_SEARCH_PARAM);
  const filters = useFiltersFromURL({ app: defaultFilters.app });

  useEffect(() => {
    // Refresh every 10 seconds.
    const interval = setInterval(refreshSessions, 10 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshSessions]);

  const resetPagination = () => {
    setPagination({
      current: 1,
      pageSize: 20,
    });
  };

  const onSubmitSearch = (search: string) => {
    queryParams.set(ACTIVE_STATEMENT_SEARCH_PARAM, search);
    history.push({
      ...history.location,
      search: queryParams.toString(),
    });
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: ActiveStatementFilters) => {
    if (selectedFilters.app === inactiveFiltersState.app) {
      queryParams.delete("app");
    } else {
      queryParams.set("app", selectedFilters.app as string);
    }
    history.push({
      ...history.location,
      search: queryParams.toString(),
    });
    resetPagination();
  };

  const onChangeSortSetting = (ss: SortSetting): void => {
    queryParams.set("ascending", ss.ascending.toString());
    queryParams.set("columnTitle", ss.columnTitle);
    history.push({
      ...history.location,
      search: queryParams.toString(),
    });
    resetPagination();
  };

  const clearSearch = () => onSubmitSearch("");
  const clearFilters = () => onSubmitFilters(inactiveFiltersState);

  const apps = appsFromActiveStatements(statements);
  const countActiveFilters = calculateActiveFilters(filters);

  const filteredStatements = statements
    .filter(stmt => filters.app === "" || stmt.application === filters.app)
    .filter(
      stmt => search == null || search === "" || stmt.query.includes(search),
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
      <div className={cx("table-area")}></div>
      <Loading
        loading={statements == null}
        page="active statements"
        error={fetchError}
        renderError={() =>
          SQLActivityError({
            statsType: "statements",
          })
        }
      >
        <ActiveStatementsSection
          filters={filters}
          pagination={pagination}
          search={search}
          statements={filteredStatements}
          selectedColumns={selectedColumns}
          onClearFilters={clearFilters}
          onChangeSortSetting={onChangeSortSetting}
          onColumnsSelect={onColumnsSelect}
        />
      </Loading>
    </div>
  );
};
