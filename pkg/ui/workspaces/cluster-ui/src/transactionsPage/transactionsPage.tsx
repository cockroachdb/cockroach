// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import styles from "../statementsPage/statementsPage.module.scss";
import { RouteComponentProps } from "react-router-dom";
import {
  makeTransactionsColumns,
  TransactionInfo,
  TransactionsTable,
} from "../transactionsTable";
import {
  ColumnDescriptor,
  handleSortSettingFromQueryString,
  ISortedTablePagination,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "../sortedtable";
import { Pagination } from "../pagination";
import { TableStatistics } from "../tableStatistics";
import { statisticsClasses } from "./transactionsPageClasses";
import {
  aggregateAcrossNodeIDs,
  generateRegionNode,
  getTrxAppFilterOptions,
  searchTransactionsData,
  filterTransactions,
} from "./utils";
import Long from "long";
import { merge } from "lodash";
import { unique, syncHistory } from "src/util";
import { EmptyTransactionsPlaceholder } from "./emptyTransactionsPlaceholder";
import { Loading } from "../loading";
import { PageConfig, PageConfigItem } from "../pageConfig";
import { Search } from "../search";
import {
  Filter,
  Filters,
  defaultFilters,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
} from "../queryFilter";
import { UIConfigState } from "../store";
import { StatementsRequest } from "src/api/statementsApi";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import ClearStats from "../sqlActivity/clearStats";
import SQLActivityError from "../sqlActivity/errorComponent";
import { commonStyles } from "../common";
import {
  TimeScaleDropdown,
  defaultTimeScaleSelected,
  TimeScale,
  toDateRange,
} from "../timeScaleDropdown";
import { InlineAlert } from "@cockroachlabs/ui-components";

type IStatementsResponse = protos.cockroach.server.serverpb.IStatementsResponse;

const cx = classNames.bind(styles);

interface TState {
  filters?: Filters;
  pagination: ISortedTablePagination;
}

export interface TransactionsPageStateProps {
  columns: string[];
  data: IStatementsResponse;
  timeScale: TimeScale;
  error?: Error | null;
  filters: Filters;
  isTenant?: UIConfigState["isTenant"];
  nodeRegions: { [nodeId: string]: string };
  pageSize?: number;
  search: string;
  sortSetting: SortSetting;
}

export interface TransactionsPageDispatchProps {
  refreshData: (req?: StatementsRequest) => void;
  resetSQLStats: () => void;
  onTimeScaleChange?: (ts: TimeScale) => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
  onFilterChange?: (value: Filters) => void;
  onSearchComplete?: (query: string) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
}

export type TransactionsPageProps = TransactionsPageStateProps &
  TransactionsPageDispatchProps &
  RouteComponentProps;

function statementsRequestFromProps(
  props: TransactionsPageProps,
): protos.cockroach.server.serverpb.StatementsRequest {
  const [start, end] = toDateRange(props.timeScale);
  return new protos.cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
  });
}
export class TransactionsPage extends React.Component<
  TransactionsPageProps,
  TState
> {
  constructor(props: TransactionsPageProps) {
    super(props);
    this.state = {
      pagination: {
        pageSize: this.props.pageSize || 20,
        current: 1,
      },
    };
    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(this.state, stateFromHistory);
  }

  getStateFromHistory = (): Partial<TState> => {
    const {
      history,
      search,
      sortSetting,
      filters,
      onSearchComplete,
      onSortingChange,
      onFilterChange,
    } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // Search query.
    const searchQuery = searchParams.get("q") || undefined;
    if (onSearchComplete && searchQuery && search != searchQuery) {
      onSearchComplete(searchQuery);
    }

    // Sort Settings.
    handleSortSettingFromQueryString(
      "Transactions",
      history.location.search,
      sortSetting,
      onSortingChange,
    );

    // Filters.
    const latestFilter = handleFiltersFromQueryString(
      history,
      filters,
      onFilterChange,
    );

    return {
      filters: latestFilter,
    };
  };

  refreshData = (): void => {
    const req = statementsRequestFromProps(this.props);
    this.props.refreshData(req);
  };

  componentDidMount(): void {
    this.refreshData();
  }

  updateQueryParams(): void {
    const { history, search, sortSetting } = this.props;
    const tab = "Transactions";

    // Search.
    const searchParams = new URLSearchParams(history.location.search);
    const currentTab = searchParams.get("tab") || "";
    const searchQueryString = searchParams.get("q") || "";
    if (currentTab === tab && search && search != searchQueryString) {
      syncHistory(
        {
          q: search,
        },
        history,
      );
    }

    // Filters.
    updateFiltersQueryParamsOnTab(tab, this.state.filters, history);

    // Sort Setting.
    updateSortSettingQueryParamsOnTab(
      tab,
      sortSetting,
      {
        ascending: false,
        columnTitle: "executionCount",
      },
      history,
    );
  }

  componentDidUpdate(): void {
    this.updateQueryParams();
  }

  onChangeSortSetting = (ss: SortSetting): void => {
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Transactions", ss.columnTitle, ss.ascending);
    }
  };

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  resetPagination = (): void => {
    this.setState((prevState: TState) => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  onClearSearchField = (): void => {
    if (this.props.onSearchComplete) {
      this.props.onSearchComplete("");
    }
    syncHistory(
      {
        q: undefined,
      },
      this.props.history,
    );
  };

  onSubmitSearchField = (search: string): void => {
    if (this.props.onSearchComplete) {
      this.props.onSearchComplete(search);
    }
    this.resetPagination();
    syncHistory(
      {
        q: search,
      },
      this.props.history,
    );
  };

  onSubmitFilters = (filters: Filters): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(filters);
    }

    this.setState({
      filters: filters,
    });
    this.resetPagination();
    syncHistory(
      {
        app: filters.app,
        timeNumber: filters.timeNumber,
        timeUnit: filters.timeUnit,
        regions: filters.regions,
        nodes: filters.nodes,
      },
      this.props.history,
    );
  };

  onClearFilters = (): void => {
    if (this.props.onFilterChange) {
      this.props.onFilterChange(defaultFilters);
    }

    this.setState({
      filters: {
        ...defaultFilters,
      },
    });
    this.resetPagination();
    syncHistory(
      {
        app: undefined,
        timeNumber: undefined,
        timeUnit: undefined,
        regions: undefined,
        nodes: undefined,
      },
      this.props.history,
    );
  };

  lastReset = (): Date => {
    return new Date(Number(this.props.data?.last_reset.seconds) * 1000);
  };

  changeTimeScale = (ts: TimeScale): void => {
    if (this.props.onTimeScaleChange) {
      this.props.onTimeScaleChange(ts);
    }
  };

  render(): React.ReactElement {
    const {
      data,
      resetSQLStats,
      nodeRegions,
      isTenant,
      onColumnsChange,
      columns: userSelectedColumnsToShow,
      sortSetting,
      search,
    } = this.props;
    const internal_app_name_prefix = data?.internal_app_name_prefix || "";
    const statements = data?.statements || [];
    const { filters } = this.state;

    // If the cluster is a tenant cluster we don't show info
    // about nodes/regions.
    const nodes = isTenant
      ? []
      : Object.keys(nodeRegions)
          .map(n => Number(n))
          .sort();
    const regions = isTenant
      ? []
      : unique(nodes.map(node => nodeRegions[node.toString()])).sort();
    // We apply the search filters and app name filters prior to aggregating across Node IDs
    // in order to match what's done on the Statements Page.
    //
    // TODO(davidh): Once the redux layer for TransactionsPage is added to this repo,
    // extract this work into the selector
    const {
      transactions: filteredTransactions,
      activeFilters,
    } = filterTransactions(
      searchTransactionsData(search, data?.transactions || [], statements),
      filters,
      internal_app_name_prefix,
      statements,
      nodeRegions,
      isTenant,
    );

    const appNames = getTrxAppFilterOptions(
      data?.transactions || [],
      internal_app_name_prefix,
    );
    const longLoadingMessage = !this.props?.data && (
      <InlineAlert
        intent="info"
        title="If the selected time period contains a large amount of data, this page might take a few minutes to load."
      />
    );

    return (
      <div className={cx("table-area")}>
        <PageConfig>
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField as any}
              onClear={this.onClearSearchField}
              defaultValue={search}
              placeholder={"Search Transactions"}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Filter
              onSubmitFilters={this.onSubmitFilters}
              appNames={appNames}
              regions={regions}
              nodes={nodes.map(n => "n" + n)}
              activeFilters={activeFilters}
              filters={filters}
              showRegions={regions.length > 1}
              showNodes={nodes.length > 1}
            />
          </PageConfigItem>
          <PageConfigItem className={commonStyles("separator")}>
            <TimeScaleDropdown
              currentScale={this.props.timeScale}
              setTimeScale={this.changeTimeScale}
            />
          </PageConfigItem>
          <PageConfigItem className={commonStyles("separator")}>
            <ClearStats
              resetSQLStats={resetSQLStats}
              tooltipType="transaction"
            />
          </PageConfigItem>
        </PageConfig>
        <Loading
          loading={!this.props?.data}
          page={"transactions"}
          error={this.props?.error}
          render={() => {
            const { pagination } = this.state;
            const transactionsToDisplay: TransactionInfo[] = aggregateAcrossNodeIDs(
              filteredTransactions,
              statements,
            ).map(t => ({
              stats_data: t.stats_data,
              node_id: t.node_id,
              regionNodes: isTenant
                ? []
                : generateRegionNode(t, statements, nodeRegions),
            }));
            const { current, pageSize } = pagination;
            const hasData = data.transactions?.length > 0;
            const isUsedFilter = search?.length > 0;

            // Creates a list of all possible columns,
            // hiding nodeRegions if is not multi-region and
            // hiding columns that won't be displayed for tenants.
            const columns = makeTransactionsColumns(
              transactionsToDisplay,
              statements,
              isTenant,
              search,
            )
              .filter(c => !(c.name === "regionNodes" && regions.length < 2))
              .filter(c => !(isTenant && c.hideIfTenant));

            const isColumnSelected = (c: ColumnDescriptor<TransactionInfo>) => {
              return (
                ((userSelectedColumnsToShow === null ||
                  userSelectedColumnsToShow === undefined) &&
                  c.showByDefault !== false) || // show column if list of visible was never defined and can be show by default.
                (userSelectedColumnsToShow !== null &&
                  userSelectedColumnsToShow.includes(c.name)) || // show column if user changed its visibility.
                c.alwaysShow === true // show column if alwaysShow option is set explicitly.
              );
            };

            // Iterate over all available columns and create list of SelectOptions with initial selection
            // values based on stored user selections in local storage and default column configs.
            // Columns that are set to alwaysShow are filtered from the list.
            const tableColumns = columns
              .filter(c => !c.alwaysShow)
              .map(
                (c): SelectOption => ({
                  label: getLabel(
                    c.name as StatisticTableColumnKeys,
                    "transaction",
                  ),
                  value: c.name,
                  isSelected: isColumnSelected(c),
                }),
              );

            // List of all columns that will be displayed based on the column selection.
            const displayColumns = columns.filter(c => isColumnSelected(c));

            return (
              <>
                <section className={statisticsClasses.tableContainerClass}>
                  <ColumnsSelector
                    options={tableColumns}
                    onSubmitColumns={onColumnsChange}
                  />
                  <TableStatistics
                    pagination={pagination}
                    search={search}
                    totalCount={transactionsToDisplay.length}
                    arrayItemName="transactions"
                    activeFilters={activeFilters}
                    onClearFilters={this.onClearFilters}
                  />
                  <TransactionsTable
                    columns={displayColumns}
                    transactions={transactionsToDisplay}
                    sortSetting={sortSetting}
                    onChangeSortSetting={this.onChangeSortSetting}
                    pagination={pagination}
                    renderNoResult={
                      <EmptyTransactionsPlaceholder
                        isEmptySearchResults={hasData && isUsedFilter}
                      />
                    }
                  />
                </section>
                <Pagination
                  pageSize={pageSize}
                  current={current}
                  total={transactionsToDisplay.length}
                  onChange={this.onChangePage}
                />
              </>
            );
          }}
          renderError={() =>
            SQLActivityError({
              statsType: "transactions",
            })
          }
        />
        {longLoadingMessage}
      </div>
    );
  }
}
