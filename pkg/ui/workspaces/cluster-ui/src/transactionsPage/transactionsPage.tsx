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
  generateRegion,
  generateRegionNode,
  getTrxAppFilterOptions,
  searchTransactionsData,
  filterTransactions,
  addInsightCounts,
} from "./utils";
import Long from "long";
import { merge } from "lodash";
import { unique, syncHistory } from "src/util";
import { EmptyTransactionsPlaceholder } from "./emptyTransactionsPlaceholder";
import { Loading } from "../loading";
import { Delayed } from "../delayed";
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
import LoadingError from "../sqlActivity/errorComponent";
import { commonStyles } from "../common";
import {
  TimeScaleDropdown,
  TimeScale,
  timeScaleToString,
  timeScale1hMinOptions,
  getValidOption,
  toRoundedDateRange,
  timeScaleRangeToObj,
} from "../timeScaleDropdown";
import { ExecutionInsightCountEvent } from "src/insights/types";
import { InlineAlert } from "@cockroachlabs/ui-components";
import { TransactionViewType } from "./transactionsPageTypes";
import { isSelectedColumn } from "../columnsSelector/utils";
import moment from "moment";
import { TxnInsightsRequest } from "../api";

type IStatementsResponse = protos.cockroach.server.serverpb.IStatementsResponse;

const cx = classNames.bind(styles);

const POLLING_INTERVAL_MILLIS = 300000;
interface TState {
  filters?: Filters;
  pagination: ISortedTablePagination;
}

export interface TransactionsPageStateProps {
  columns: string[];
  data: IStatementsResponse;
  isDataValid: boolean;
  lastUpdated: moment.Moment | null;
  timeScale: TimeScale;
  error?: Error | null;
  filters: Filters;
  isTenant?: UIConfigState["isTenant"];
  nodeRegions: { [nodeId: string]: string };
  pageSize?: number;
  search: string;
  sortSetting: SortSetting;
  hasAdminRole?: UIConfigState["hasAdminRole"];
  insightCounts: ExecutionInsightCountEvent[];
}

export interface TransactionsPageDispatchProps {
  refreshData: (req: StatementsRequest) => void;
  refreshNodes: () => void;
  refreshUserSQLRoles: () => void;
  refreshInsightCount: (req: TxnInsightsRequest) => void;
  resetSQLStats: (req: StatementsRequest) => void;
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

function stmtsRequestFromTimeScale(
  ts: TimeScale,
): protos.cockroach.server.serverpb.StatementsRequest {
  const [start, end] = toRoundedDateRange(ts);
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
  refreshDataTimeout: NodeJS.Timeout;

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

    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(this.props.timeScale, timeScale1hMinOptions);
    if (ts !== this.props.timeScale) {
      this.changeTimeScale(ts);
    }
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

  clearRefreshDataTimeout(): void {
    if (this.refreshDataTimeout !== null) {
      clearTimeout(this.refreshDataTimeout);
    }
  }

  // Schedule the next data request depending on the time
  // range key.
  resetPolling(ts: TimeScale): void {
    this.clearRefreshDataTimeout();
    if (ts.key !== "Custom") {
      this.refreshDataTimeout = setTimeout(
        this.refreshData,
        POLLING_INTERVAL_MILLIS, // 5 minutes
        ts,
      );
    }
  }

  refreshData = (ts?: TimeScale): void => {
    const time = ts ?? this.props.timeScale;
    const req = stmtsRequestFromTimeScale(time);
    const insightCountReq = timeScaleRangeToObj(time);
    this.props.refreshInsightCount(insightCountReq);
    this.props.refreshData(req);

    this.resetPolling(time);
  };

  resetSQLStats = (): void => {
    const req = stmtsRequestFromTimeScale(this.props.timeScale);
    this.props.resetSQLStats(req);
    this.resetPolling(this.props.timeScale);
  };

  componentDidMount(): void {
    // For the first data fetch for this page, we refresh immediately if:
    // - Last updated is null (no statements fetched previously)
    // - The data is not valid (time scale may have changed on other pages)
    // - The time range selected is a moving window and the last udpated time
    // is >= 5 minutes.
    // Otherwise, we schedule a refresh at 5 mins from the lastUpdated time if
    // the time range selected is a moving window (i.e. not custom).
    const now = moment();
    let nextRefresh = null;
    if (this.props.lastUpdated == null || !this.props.isDataValid) {
      nextRefresh = now;
    } else if (this.props.timeScale.key !== "Custom") {
      nextRefresh = this.props.lastUpdated
        .clone()
        .add(POLLING_INTERVAL_MILLIS, "milliseconds");
    }
    if (nextRefresh) {
      this.refreshDataTimeout = setTimeout(
        this.refreshData,
        Math.max(0, nextRefresh.diff(now, "milliseconds")),
      );
    }

    if (!this.props.insightCounts) {
      const insightCountReq = timeScaleRangeToObj(this.props.timeScale);
      this.props.refreshInsightCount(insightCountReq);
    }

    this.props.refreshNodes();
    this.props.refreshUserSQLRoles();
  }

  componentWillUnmount(): void {
    this.clearRefreshDataTimeout();
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
    this.props.refreshNodes();
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
    this.refreshData(ts);
  };

  render(): React.ReactElement {
    const {
      data,
      nodeRegions,
      isTenant,
      onColumnsChange,
      columns: userSelectedColumnsToShow,
      sortSetting,
      search,
      hasAdminRole,
      insightCounts,
    } = this.props;
    const internal_app_name_prefix = data?.internal_app_name_prefix || "";
    const statements = data?.statements || [];
    const { filters } = this.state;

    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();

    const regions = unique(
      nodes.map(node => nodeRegions[node.toString()]),
    ).sort();

    // We apply the search filters and app name filters prior to aggregating across Node IDs
    // in order to match what's done on the Statements Page.
    //
    // TODO(davidh): Once the redux layer for TransactionsPage is added to this repo,
    // extract this work into the selector
    const { transactions: filteredTransactions, activeFilters } =
      filterTransactions(
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
      <Delayed delay={moment.duration(2, "s")}>
        <InlineAlert
          intent="info"
          title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
        />
      </Delayed>
    );

    return (
      <>
        <PageConfig>
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField}
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
              timeLabel={"Transaction fingerprint"}
              nodes={nodes.map(n => "n" + n)}
              activeFilters={activeFilters}
              filters={filters}
              showRegions={regions.length > 1}
              showNodes={!isTenant && nodes.length > 1}
            />
          </PageConfigItem>
          <PageConfigItem className={commonStyles("separator")}>
            <TimeScaleDropdown
              options={timeScale1hMinOptions}
              currentScale={this.props.timeScale}
              setTimeScale={this.changeTimeScale}
            />
          </PageConfigItem>
          {hasAdminRole && (
            <PageConfigItem className={commonStyles("separator")}>
              <ClearStats
                resetSQLStats={this.resetSQLStats}
                tooltipType="transaction"
              />
            </PageConfigItem>
          )}
        </PageConfig>
        <div className={cx("table-area")}>
          <Loading
            loading={!this.props?.data}
            page={"transactions"}
            error={this.props?.error}
            render={() => {
              const { pagination } = this.state;
              const transactionsToDisplay: TransactionInfo[] = addInsightCounts(
                aggregateAcrossNodeIDs(filteredTransactions, statements).map(
                  t => ({
                    stats_data: t.stats_data,
                    node_id: t.node_id,
                    regions: generateRegion(t, statements, nodeRegions),
                    regionNodes: generateRegionNode(t, statements, nodeRegions),
                  }),
                ),
                insightCounts,
              );
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
                .filter(c => !(c.name === "regions" && regions.length < 2))
                .filter(c => !(c.name === "regionNodes" && regions.length < 2))
                .filter(c => !(isTenant && c.hideIfTenant));

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
                    isSelected: isSelectedColumn(userSelectedColumnsToShow, c),
                  }),
                );

              // List of all columns that will be displayed based on the column selection.
              const displayColumns = columns.filter(c =>
                isSelectedColumn(userSelectedColumnsToShow, c),
              );

              const period = timeScaleToString(this.props.timeScale);

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
                      period={period}
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
                          transactionView={TransactionViewType.FINGERPRINTS}
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
              LoadingError({
                statsType: "transactions",
                timeout: this.props?.error?.name
                  ?.toLowerCase()
                  .includes("timeout"),
              })
            }
          />
          {longLoadingMessage}
        </div>
      </>
    );
  }
}
