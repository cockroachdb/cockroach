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
import {
  ActiveTransaction,
  ActiveStatementFilters,
} from "src/activeExecutions";
import { defaultFilters, Filter } from "src/queryFilter";
import SQLActivityError from "src/sqlActivity/errorComponent";
import { ACTIVE_STATEMENT_SEARCH_PARAM } from "../activeExecutions/activeStatementUtils";
import { useQueryParmeters } from "../hooks/useQueryParameters";
import { calculateActiveFilters } from "../queryFilter/filter";
import { useFiltersFromURL } from "../queryFilter/useFiltersFromURL";
import { appsFromActiveTransactions } from "../activeExecutions/activeStatementUtils";
import { inactiveFiltersState } from "../queryFilter/filter";
import { ActiveTransactionsSection } from "src/activeExecutions/activeTransactionsSection";

import styles from "../statementsPage/statementsPage.module.scss";
const cx = classNames.bind(styles);

export type ActiveTransactionsViewDispatchProps = {
  onColumnsSelect: (columns: string[]) => void;
  refreshSessions: () => void;
};

export type ActiveTransactionsViewStateProps = {
  selectedColumns: string[];
  transactions: ActiveTransaction[];
  fetchError: Error | null;
};

export type ActiveTransactionsViewProps = ActiveTransactionsViewStateProps &
  ActiveTransactionsViewDispatchProps;

export const ActiveTransactionsView: React.FC<ActiveTransactionsViewProps> = ({
  onColumnsSelect,
  refreshSessions,
  selectedColumns,
  transactions,
  fetchError,
}: ActiveTransactionsViewProps) => {
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

  const onChangeSortSetting = (ss: SortSetting): void => {
    queryParams.set("ascending", ss.ascending.toString());
    queryParams.set("columnTitle", ss.columnTitle);
    history.push({
      ...history.location,
      search: queryParams.toString(),
    });
    resetPagination();
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

  const clearSearch = () => onSubmitSearch("");
  const clearFilters = () => onSubmitFilters(inactiveFiltersState);

  const apps = appsFromActiveTransactions(transactions);
  const countActiveFilters = calculateActiveFilters(filters);

  const filteredTransactions = transactions
    .filter(txn => filters.app === "" || txn.application === filters.app)
    .filter(
      txn =>
        search == null ||
        search === "" ||
        txn.mostRecentStatement?.query.includes(search),
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
        loading={transactions == null}
        page="active transactions"
        error={fetchError}
        renderError={() =>
          SQLActivityError({
            statsType: "transactions",
          })
        }
      >
        <ActiveTransactionsSection
          filters={filters}
          pagination={pagination}
          search={search}
          transactions={filteredTransactions}
          selectedColumns={selectedColumns}
          onClearFilters={clearFilters}
          onChangeSortSetting={onChangeSortSetting}
          onColumnsSelect={onColumnsSelect}
        />
      </Loading>
    </div>
  );
};
