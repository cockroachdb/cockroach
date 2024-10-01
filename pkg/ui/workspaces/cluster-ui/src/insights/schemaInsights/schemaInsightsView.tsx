// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React, { useContext, useEffect, useState } from "react";
import { useHistory } from "react-router-dom";

import { Anchor } from "src/anchor";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import styles from "src/statementsPage/statementsPage.module.scss";
import { insights } from "src/util";

import { CockroachCloudContext } from "../../contexts";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../../insightsTable/insightsTable";
import insightTableStyles from "../../insightsTable/insightsTable.module.scss";
import { Loading } from "../../loading";
import { PageConfig, PageConfigItem } from "../../pageConfig";
import { Pagination } from "../../pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  getFullFiltersAsStringRecord,
  SelectedFilters,
} from "../../queryFilter";
import { getSchemaInsightEventFiltersFromURL } from "../../queryFilter/utils";
import { Search } from "../../search";
import { ISortedTablePagination, SortSetting } from "../../sortedtable";
import { getTableSortFromURL } from "../../sortedtable/getTableSortFromURL";
import { TableStatistics } from "../../tableStatistics";
import { queryByName, syncHistory } from "../../util";
import { InsightsError } from "../insightsErrorComponent";
import { InsightRecommendation, SchemaInsightEventFilters } from "../types";
import { filterSchemaInsights } from "../utils";

import { EmptySchemaInsightsTablePlaceholder } from "./emptySchemaInsightsTablePlaceholder";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);
const insightTableCx = classNames.bind(insightTableStyles);

export type SchemaInsightsViewStateProps = {
  schemaInsights: InsightRecommendation[];
  schemaInsightsDatabases: string[];
  schemaInsightsTypes: string[];
  schemaInsightsError: Error | null;
  filters: SchemaInsightEventFilters;
  sortSetting: SortSetting;
  hasAdminRole: boolean;
  csIndexUnusedDuration: string;
  maxSizeApiReached?: boolean;
};

export type SchemaInsightsViewDispatchProps = {
  onFiltersChange: (filters: SchemaInsightEventFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshSchemaInsights: (csIndexUnusedDuration: string) => void;
  refreshUserSQLRoles: () => void;
};

export type SchemaInsightsViewProps = SchemaInsightsViewStateProps &
  SchemaInsightsViewDispatchProps;

const SCHEMA_INSIGHT_SEARCH_PARAM = "q";

export const SchemaInsightsView: React.FC<SchemaInsightsViewProps> = ({
  sortSetting,
  schemaInsights,
  schemaInsightsDatabases,
  schemaInsightsTypes,
  schemaInsightsError,
  filters,
  hasAdminRole,
  refreshSchemaInsights,
  refreshUserSQLRoles,
  onFiltersChange,
  onSortChange,
  maxSizeApiReached,
  csIndexUnusedDuration,
}: SchemaInsightsViewProps) => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 10,
  });
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, SCHEMA_INSIGHT_SEARCH_PARAM),
  );

  useEffect(() => {
    const refreshSchema = (): void => {
      refreshSchemaInsights(csIndexUnusedDuration);
    };

    // Refresh every 1 minute.
    refreshSchema();
    const interval = setInterval(refreshSchema, 60 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshSchemaInsights, csIndexUnusedDuration]);

  useEffect(() => {
    // Refresh every 5 minutes.
    refreshUserSQLRoles();
    const interval = setInterval(refreshUserSQLRoles, 60 * 5000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshUserSQLRoles]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getSchemaInsightEventFiltersFromURL(
      history.location,
    );

    if (sortSettingURL) {
      onSortChange(sortSettingURL);
    }
    if (filtersFromURL) {
      onFiltersChange(filtersFromURL);
    }
  }, [history, onFiltersChange, onSortChange]);

  useEffect(() => {
    // This effect runs when the filters or sort settings received from
    // redux changes and syncs the URL params with redux.
    syncHistory(
      {
        ascending: sortSetting.ascending?.toString(),
        columnTitle: sortSetting.columnTitle,
        ...getFullFiltersAsStringRecord(filters),
        [SCHEMA_INSIGHT_SEARCH_PARAM]: search,
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

  const onSubmitFilters = (selectedFilters: SchemaInsightEventFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const clearFilters = () =>
    onSubmitFilters({
      database: defaultFilters.database,
      schemaInsightType: defaultFilters.schemaInsightType,
    });

  const countActiveFilters = calculateActiveFilters(filters);

  const filteredSchemaInsights = filterSchemaInsights(
    schemaInsights,
    filters,
    search,
  );

  return (
    <div className={cx("root")}>
      <PageConfig>
        <PageConfigItem>
          <Search
            placeholder="Search Schema Insights"
            onSubmit={onSubmitSearch}
            onClear={clearSearch}
            defaultValue={search}
          />
        </PageConfigItem>
        <PageConfigItem>
          <Filter
            activeFilters={countActiveFilters}
            onSubmitFilters={onSubmitFilters}
            filters={filters}
            hideAppNames={true}
            dbNames={schemaInsightsDatabases}
            schemaInsightTypes={schemaInsightsTypes}
            showDB={true}
            showSchemaInsightTypes={true}
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
          loading={schemaInsights === null}
          page="schema insights"
          error={schemaInsightsError}
          renderError={() => InsightsError(schemaInsightsError?.message)}
        >
          <div>
            <section className={sortableTableCx("cl-table-container")}>
              <div>
                <TableStatistics
                  pagination={pagination}
                  search={search}
                  totalCount={filteredSchemaInsights?.length}
                  arrayItemName="schema insights"
                  activeFilters={countActiveFilters}
                />
              </div>
              <InsightsSortedTable
                columns={makeInsightsColumns(
                  isCockroachCloud,
                  hasAdminRole,
                  false,
                )}
                data={filteredSchemaInsights}
                sortSetting={sortSetting}
                onChangeSortSetting={onChangeSortSetting}
                pagination={pagination}
                renderNoResult={
                  <EmptySchemaInsightsTablePlaceholder
                    isEmptySearchResults={
                      search?.length > 0 && filteredSchemaInsights?.length === 0
                    }
                  />
                }
                tableWrapperClassName={insightTableCx("sorted-table")}
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              current={pagination.current}
              total={filteredSchemaInsights?.length}
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
