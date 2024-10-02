// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useEffect, useState, useCallback } from "react";
import { useHistory } from "react-router-dom";

import { Anchor } from "src/anchor";
import { StmtInsightsReq } from "src/api/stmtInsightsApi";
import { isSelectedColumn } from "src/columnsSelector/utils";
import {
  filterStatementInsights,
  StmtInsightEvent,
  getAppsFromStatementInsights,
  makeStatementInsightsColumns,
  WorkloadInsightEventFilters,
} from "src/insights";
import { Loading } from "src/loading/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig/pageConfig";
import { Pagination } from "src/pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  getFullFiltersAsStringRecord,
  SelectedFilters,
} from "src/queryFilter/filter";
import { getWorkloadInsightEventFiltersFromURL } from "src/queryFilter/utils";
import { Search } from "src/search/search";
import { getTableSortFromURL } from "src/sortedtable/getTableSortFromURL";
import {
  ISortedTablePagination,
  SortSetting,
} from "src/sortedtable/sortedtable";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import styles from "src/statementsPage/statementsPage.module.scss";
import { TableStatistics } from "src/tableStatistics";
import { insights } from "src/util";
import { useScheduleFunction } from "src/util/hooks";
import { queryByName, syncHistory } from "src/util/query";

import ColumnsSelector from "../../../columnsSelector/columnsSelector";
import { commonStyles } from "../../../common";
import { SelectOption } from "../../../multiSelectCheckbox/multiSelectCheckbox";
import {
  defaultTimeScaleOptions,
  TimeScale,
  TimeScaleDropdown,
  timeScaleRangeToObj,
} from "../../../timeScaleDropdown";
import { InsightsError } from "../../insightsErrorComponent";
import { EmptyInsightsTablePlaceholder } from "../util";

import { StatementInsightsTable } from "./statementInsightsTable";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

export type StatementInsightsViewStateProps = {
  filters: WorkloadInsightEventFilters;
  insightTypes: string[];
  isDataValid: boolean;
  lastUpdated: moment.Moment;
  selectedColumnNames: string[];
  sortSetting: SortSetting;
  statements: StmtInsightEvent[];
  statementsError: Error | null;
  dropDownSelect?: React.ReactElement;
  isLoading?: boolean;
  maxSizeApiReached?: boolean;
  timeScale?: TimeScale;
};

export type StatementInsightsViewDispatchProps = {
  onFiltersChange: (filters: WorkloadInsightEventFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshStatementInsights: (req: StmtInsightsReq) => void;
  onColumnsChange: (selectedColumns: string[]) => void;
  setTimeScale: (ts: TimeScale) => void;
};

export type StatementInsightsViewProps = StatementInsightsViewStateProps &
  StatementInsightsViewDispatchProps;

const INSIGHT_STMT_SEARCH_PARAM = "q";
const INTERNAL_APP_NAME_PREFIX = "$ internal";

export const StatementInsightsView: React.FC<StatementInsightsViewProps> = ({
  isDataValid,
  lastUpdated,
  sortSetting,
  statements,
  statementsError,
  insightTypes,
  filters,
  timeScale,
  isLoading,
  refreshStatementInsights,
  onFiltersChange,
  onSortChange,
  onColumnsChange,
  setTimeScale,
  selectedColumnNames,
  dropDownSelect,
  maxSizeApiReached,
}: StatementInsightsViewProps) => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 10,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, INSIGHT_STMT_SEARCH_PARAM),
  );

  const refresh = useCallback(() => {
    const ts = timeScaleRangeToObj(timeScale);
    const req = {
      start: ts.start,
      end: ts.end,
    };
    refreshStatementInsights(req);
  }, [refreshStatementInsights, timeScale]);

  const shouldPoll = timeScale.key !== "Custom";
  const [refetch, clearPolling] = useScheduleFunction(
    refresh,
    shouldPoll, // Don't reschedule refresh if we have a custom time interval.
    10 * 1000, // 10s polling interval
    lastUpdated,
  );

  useEffect(() => {
    if (!isDataValid) refetch();
  }, [isDataValid, refetch]);

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

  const defaultColumns = makeStatementInsightsColumns();

  const onSetTimeScale = useCallback(
    (ts: TimeScale) => {
      clearPolling();
      setTimeScale(ts);
    },
    [setTimeScale, clearPolling],
  );

  const visibleColumns = defaultColumns.filter(x =>
    isSelectedColumn(selectedColumnNames, x),
  );

  const clearFilters = () =>
    onSubmitFilters({
      app: defaultFilters.app,
      workloadInsightType: defaultFilters.workloadInsightType,
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
        <PageConfigItem>{dropDownSelect}</PageConfigItem>
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
            workloadInsightTypes={insightTypes.sort()}
            showWorkloadInsightTypes={true}
          />
        </PageConfigItem>
        <PageConfigItem className={commonStyles("separator")}>
          <TimeScaleDropdown
            options={defaultTimeScaleOptions}
            currentScale={timeScale}
            setTimeScale={onSetTimeScale}
          />
        </PageConfigItem>
      </PageConfig>
      <SelectedFilters
        filters={filters}
        onRemoveFilter={onSubmitFilters}
        onClearFilters={clearFilters}
        className={cx("margin-adjusted")}
      />
      <div className={cx("table-area")}>
        <Loading
          loading={isLoading}
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
                  totalCount={filteredStatements?.length}
                  arrayItemName="statement insights"
                  activeFilters={countActiveFilters}
                />
              </div>
              <StatementInsightsTable
                data={filteredStatements}
                sortSetting={sortSetting}
                visibleColumns={visibleColumns}
                onChangeSortSetting={onChangeSortSetting}
                renderNoResult={
                  <EmptyInsightsTablePlaceholder
                    isEmptySearchResults={
                      (search?.length > 0 || countActiveFilters > 0) &&
                      filteredStatements?.length === 0
                    }
                  />
                }
                pagination={pagination}
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              current={pagination.current}
              total={filteredStatements?.length}
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
