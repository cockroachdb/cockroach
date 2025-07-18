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
import { insights, usePagination } from "src/util";

import { useSchemaInsights } from "../../api";
import { useUserSQLRoles } from "../../api/userApi";
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
import { SortSetting } from "../../sortedtable";
import { getTableSortFromURL } from "../../sortedtable/getTableSortFromURL";
import { TableStatistics } from "../../tableStatistics";
import { queryByName, syncHistory } from "../../util";
import { InsightsError } from "../insightsErrorComponent";
import { SchemaInsightEventFilters } from "../types";
import { filterSchemaInsights, insightType } from "../utils";

import { EmptySchemaInsightsTablePlaceholder } from "./emptySchemaInsightsTablePlaceholder";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);
const insightTableCx = classNames.bind(insightTableStyles);

const SCHEMA_INSIGHT_SEARCH_PARAM = "q";

export const SchemaInsightsView: React.FC = () => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  const [pagination, updatePagination, resetPagination] = usePagination(1, 10);
  const history = useHistory();
  const [search, setSearch] = useState<string>(
    queryByName(history.location, SCHEMA_INSIGHT_SEARCH_PARAM),
  );
  const { data: roles } = useUserSQLRoles();
  const [hasAdminRole, setHasAdminRole] = useState(false);
  const [sortSetting, setSortSetting] = useState<SortSetting>(
    {} as SortSetting,
  );
  const [filters, setFilters] = useState<SchemaInsightEventFilters>({});
  const { data, error: schemaInsightsError, isLoading } = useSchemaInsights();

  const [dbs, setDbs] = useState<string[]>([]);
  const [types, setTypes] = useState<string[]>([]);

  // Check if the user has the admin role anytime roles updates.
  useEffect(() => {
    if (roles?.roles?.includes("ADMIN")) {
      setHasAdminRole(true);
    }
  }, [roles]);

  // Extract unique databases and insight types from the schema insights data.
  useEffect(() => {
    const insightDbs = new Set<string>();
    const insightTypes = new Set<string>();
    data?.results.forEach(insight => {
      insightDbs.add(insight.database);
      insightTypes.add(insightType(insight.type));
    });
    setDbs(Array.from(insightDbs));
    setTypes(Array.from(insightTypes));
  }, [data]);

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the state. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behavior is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getSchemaInsightEventFiltersFromURL(
      history.location,
    );

    if (sortSettingURL) {
      setSortSetting(sortSettingURL);
    }
    if (filtersFromURL) {
      setFilters(filtersFromURL);
    }
  }, [history]);

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

  const onChangeSortSetting = (ss: SortSetting): void => {
    setSortSetting(ss);
    resetPagination();
  };

  const onSubmitSearch = (newSearch: string) => {
    if (newSearch === search) return;
    setSearch(newSearch);
    resetPagination();
  };

  const clearSearch = () => onSubmitSearch("");

  const onSubmitFilters = (selectedFilters: SchemaInsightEventFilters) => {
    setFilters(selectedFilters);
    resetPagination();
  };

  const clearFilters = () =>
    onSubmitFilters({
      database: defaultFilters.database,
      schemaInsightType: defaultFilters.schemaInsightType,
    });

  const countActiveFilters = calculateActiveFilters(filters);

  const filteredSchemaInsights = filterSchemaInsights(
    data?.results || [],
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
            dbNames={dbs}
            schemaInsightTypes={types}
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
          loading={isLoading}
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
              onChange={updatePagination}
              onShowSizeChange={updatePagination}
            />
            {data?.maxSizeReached && (
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
