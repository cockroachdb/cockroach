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
import {
  calculateActiveFilters,
  Filter,
  getFullFiltersAsStringRecord,
} from "src/queryFilter/filter";
import { getWorkloadInsightEventFiltersFromURL } from "src/queryFilter/utils";
import { Pagination } from "src/pagination";
import { queryByName, syncHistory } from "src/util/query";
import { getTableSortFromURL } from "src/sortedtable/getTableSortFromURL";
import { TableStatistics } from "src/tableStatistics";
import { isSelectedColumn } from "src/columnsSelector/utils";

import { StatementInsights } from "src/api/insightsApi";
import {
  filterStatementInsights,
  getAppsFromStatementInsights,
  makeStatementInsightsColumns,
  WorkloadInsightEventFilters,
} from "src/insights";
import { EmptyInsightsTablePlaceholder } from "../util";
import { StatementInsightsTable } from "./statementInsightsTable";
import { InsightsError } from "../../insightsErrorComponent";

import styles from "src/statementsPage/statementsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import ColumnsSelector from "../../../columnsSelector/columnsSelector";
import { SelectOption } from "../../../multiSelectCheckbox/multiSelectCheckbox";
import { TimeScale } from "../../../timeScaleDropdown";
import { InlineAlert } from "@cockroachlabs/ui-components";
import { insights } from "src/util";
import { Anchor } from "src/anchor";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

export type StatementInsightsViewStateProps = {
  statements: StatementInsights;
  statementsError: Error | null;
  filters: WorkloadInsightEventFilters;
  sortSetting: SortSetting;
  selectedColumnNames: string[];
  dropDownSelect?: React.ReactElement;
  isTenant?: boolean;
  maxSizeApiReached?: boolean;
};

export type StatementInsightsViewDispatchProps = {
  onFiltersChange: (filters: WorkloadInsightEventFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshStatementInsights: () => void;
  onColumnsChange: (selectedColumns: string[]) => void;
  setTimeScale: (ts: TimeScale) => void;
};

export type StatementInsightsViewProps = StatementInsightsViewStateProps &
  StatementInsightsViewDispatchProps;

const INSIGHT_STMT_SEARCH_PARAM = "q";
const INTERNAL_APP_NAME_PREFIX = "$ internal";

export const StatementInsightsView: React.FC<StatementInsightsViewProps> = (
  props: StatementInsightsViewProps,
) => {
  const {
    sortSetting,
    statements,
    statementsError,
    filters,
    refreshStatementInsights,
    onFiltersChange,
    onSortChange,
    onColumnsChange,
    setTimeScale,
    selectedColumnNames,
    dropDownSelect,
    isTenant,
    maxSizeApiReached,
  } = props;

  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 10,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, INSIGHT_STMT_SEARCH_PARAM),
  );

  useEffect(() => {
    // Refresh every 10 seconds.
    refreshStatementInsights();
    const interval = setInterval(refreshStatementInsights, 10 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshStatementInsights]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getWorkloadInsightEventFiltersFromURL(
      history.location,
    );

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
        ...getFullFiltersAsStringRecord(filters),
        [INSIGHT_STMT_SEARCH_PARAM]: search,
      },
      history,
    );
  }, [
    history,
    filters,
    sortSetting.ascending,
    sortSetting.columnTitle,
    search,
  ]);

  const onChangePage = (current: number): void => {
    setPagination({
      current: current,
      pageSize: 10,
    });
  };

  const resetPagination = () => {
    setPagination({
      current: 1,
      pageSize: 10,
    });
  };

  const onChangeSortSetting = (ss: SortSetting): void => {
    onSortChange(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string) => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const clearSearch = () => onSubmitSearch("");

  const onSubmitFilters = (selectedFilters: WorkloadInsightEventFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const defaultColumns = makeStatementInsightsColumns(setTimeScale);

  const visibleColumns = defaultColumns.filter(x =>
    isSelectedColumn(selectedColumnNames, x),
  );

  const clearFilters = () =>
    onSubmitFilters({
      app: "",
    });

  const apps = getAppsFromStatementInsights(
    statements,
    INTERNAL_APP_NAME_PREFIX,
  );
  const countActiveFilters = calculateActiveFilters(filters);
  const filteredStatements = filterStatementInsights(
    statements,
    filters,
    INTERNAL_APP_NAME_PREFIX,
    search,
  );

  // const statementInsights =
  //   populateStatementInsightsFromProblemAndCauses(filteredStatements);
  const statementInsights = filteredStatements;

  const tableColumns = defaultColumns
    .filter(c => !c.alwaysShow)
    .map(
      (c): SelectOption => ({
        label: (c.title as React.ReactElement).props.children,
        value: c.name,
        isSelected: isSelectedColumn(selectedColumnNames, c),
      }),
    );

  return (
    <div className={cx("root")}>
      <PageConfig>
        {!isTenant && <PageConfigItem>{dropDownSelect}</PageConfigItem>}
        <PageConfigItem>
          <Search
            placeholder="Search Statements"
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
          loading={statements === null}
          page="statement insights"
          error={statementsError}
          renderError={() => InsightsError(statementsError?.message)}
        >
          <div>
            <section className={sortableTableCx("cl-table-container")}>
              <div>
                <ColumnsSelector
                  options={tableColumns}
                  onSubmitColumns={onColumnsChange}
                  size={"small"}
                />
                <TableStatistics
                  pagination={pagination}
                  search={search}
                  totalCount={statementInsights?.length}
                  arrayItemName="statement insights"
                  activeFilters={countActiveFilters}
                  onClearFilters={clearFilters}
                />
              </div>
              <StatementInsightsTable
                data={statementInsights}
                sortSetting={sortSetting}
                visibleColumns={visibleColumns}
                onChangeSortSetting={onChangeSortSetting}
                renderNoResult={
                  <EmptyInsightsTablePlaceholder
                    isEmptySearchResults={
                      search?.length > 0 && statementInsights?.length === 0
                    }
                  />
                }
                pagination={pagination}
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              current={pagination.current}
              total={statementInsights?.length}
              onChange={onChangePage}
            />
            {maxSizeApiReached && (
              <InlineAlert
                intent="info"
                title={
                  <>
                    Not all insights are displayed because the maximum number of
                    insights was reached in the console.&nbsp;
                    <Anchor href={insights} target="_blank">
                      Learn more
                    </Anchor>
                  </>
                }
              />
            )}
          </div>
        </Loading>
      </div>
    </div>
  );
};
