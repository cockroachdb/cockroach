// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import styles from "src/statementsPage/statementsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { ISortedTablePagination, SortSetting } from "../../sortedtable";
import classNames from "classnames/bind";
import { PageConfig, PageConfigItem } from "../../pageConfig";
import { Loading } from "../../loading";
import { useEffect, useState } from "react";
import { useHistory } from "react-router-dom";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "../../insightsTable/insightsTable";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  getFullFiltersAsStringRecord,
} from "../../queryFilter";
import { queryByName, syncHistory } from "../../util";
import { getTableSortFromURL } from "../../sortedtable/getTableSortFromURL";
import { TableStatistics } from "../../tableStatistics";
import { InsightRecommendation, SchemaInsightEventFilters } from "../types";
import { getSchemaInsightEventFiltersFromURL } from "../../queryFilter/utils";
import { filterSchemaInsights } from "../utils";
import { Search } from "../../search";
import { InsightsError } from "../insightsErrorComponent";
import { Pagination } from "../../pagination";
import { EmptySchemaInsightsTablePlaceholder } from "./emptySchemaInsightsTablePlaceholder";
import { CockroachCloudContext } from "../../contexts";
const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

export type SchemaInsightsViewStateProps = {
  schemaInsights: InsightRecommendation[];
  schemaInsightsDatabases: string[];
  schemaInsightsTypes: string[];
  schemaInsightsError: Error | null;
  filters: SchemaInsightEventFilters;
  sortSetting: SortSetting;
};

export type SchemaInsightsViewDispatchProps = {
  onFiltersChange: (filters: SchemaInsightEventFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  refreshSchemaInsights: () => void;
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
  refreshSchemaInsights,
  onFiltersChange,
  onSortChange,
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
    // Refresh every 1 minute.
    refreshSchemaInsights();
    const interval = setInterval(refreshSchemaInsights, 60 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshSchemaInsights]);

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
        ascending: sortSetting.ascending.toString(),
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
      <div className={cx("table-area")}>
        <Loading
          loading={schemaInsights === null}
          page="schema insights"
          error={schemaInsightsError}
          renderError={() => InsightsError()}
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
                  onClearFilters={clearFilters}
                />
              </div>
              <InsightsSortedTable
                columns={makeInsightsColumns(isCockroachCloud)}
                data={filteredSchemaInsights}
                sortSetting={sortSetting}
                onChangeSortSetting={onChangeSortSetting}
                renderNoResult={
                  <EmptySchemaInsightsTablePlaceholder
                    isEmptySearchResults={
                      search?.length > 0 && filteredSchemaInsights?.length === 0
                    }
                  />
                }
              />
            </section>
            <Pagination
              pageSize={pagination.pageSize}
              current={pagination.current}
              total={filteredSchemaInsights?.length}
              onChange={onChangePage}
            />
          </div>
        </Loading>
      </div>
    </div>
  );
};
