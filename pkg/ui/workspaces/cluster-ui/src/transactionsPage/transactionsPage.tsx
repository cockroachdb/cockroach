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
import moment, { Moment } from "moment";
import { RouteComponentProps } from "react-router-dom";
import {
  makeTransactionsColumns,
  TransactionInfo,
  TransactionsTable,
} from "../transactionsTable";
import { DateRange } from "src/dateRange";
import { TransactionDetails } from "../transactionDetails";
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
  statementFingerprintIdsToText,
  searchTransactionsData,
  filterTransactions,
  getStatementsByFingerprintIdAndTime,
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

type IStatementsResponse = protos.cockroach.server.serverpb.IStatementsResponse;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;
type Timestamp = protos.google.protobuf.ITimestamp;

const cx = classNames.bind(styles);

interface TState {
  pagination: ISortedTablePagination;
  search?: string;
  filters?: Filters;
  statementFingerprintIds: Long[] | null;
  aggregatedTs: Timestamp | null;
  transactionStats: TransactionStats | null;
  transactionFingerprintId: Long | null;
}

export interface TransactionsPageStateProps {
  data: IStatementsResponse;
  dateRange: [Moment, Moment];
  nodeRegions: { [nodeId: string]: string };
  error?: Error | null;
  pageSize?: number;
  isTenant?: UIConfigState["isTenant"];
  columns: string[];
  sortSetting: SortSetting;
  filters: Filters;
}

export interface TransactionsPageDispatchProps {
  refreshData: (req?: StatementsRequest) => void;
  resetSQLStats: () => void;
  onDateRangeChange?: (start: Moment, end: Moment) => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
  onFilterChange?: (value: Filters) => void;
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
  return new protos.cockroach.server.serverpb.StatementsRequest({
    combined: true,
    start: Long.fromNumber(props.dateRange[0].unix()),
    end: Long.fromNumber(props.dateRange[1].unix()),
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
      search: "",
      aggregatedTs: null,
      statementFingerprintIds: null,
      transactionStats: null,
      transactionFingerprintId: null,
    };
    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(this.state, stateFromHistory);
  }

  getStateFromHistory = (): Partial<TState> => {
    const { history, sortSetting, filters } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // Search query.
    const searchQuery = searchParams.get("q") || undefined;

    // Sort Settings.
    handleSortSettingFromQueryString(
      "Transactions",
      history.location.search,
      sortSetting,
      this.props.onSortingChange,
    );

    // Filters.
    const latestFilter = handleFiltersFromQueryString(
      history,
      filters,
      this.props.onFilterChange,
    );

    return {
      search: searchQuery,
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
    updateFiltersQueryParamsOnTab(
      "Transactions",
      this.state.filters,
      this.props.history,
    );

    updateSortSettingQueryParamsOnTab(
      "Transactions",
      this.props.sortSetting,
      {
        ascending: false,
        columnTitle: "executionCount",
      },
      this.props.history,
    );
  }

  componentDidUpdate(): void {
    this.updateQueryParams();
    this.refreshData();
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
    this.setState({ search: "" });
    syncHistory(
      {
        q: undefined,
      },
      this.props.history,
    );
  };

  onSubmitSearchField = (search: string): void => {
    this.setState({ search });
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

  handleDetails = (transaction?: TransactionInfo): void => {
    this.setState({
      statementFingerprintIds:
        transaction?.stats_data?.statement_fingerprint_ids,
      transactionStats: transaction?.stats_data?.stats,
      aggregatedTs: transaction?.stats_data?.aggregated_ts,
      transactionFingerprintId:
        transaction?.stats_data?.transaction_fingerprint_id,
    });
  };

  lastReset = (): Date => {
    return new Date(Number(this.props.data?.last_reset.seconds) * 1000);
  };

  changeDateRange = (start: Moment, end: Moment): void => {
    if (this.props.onDateRangeChange) {
      this.props.onDateRangeChange(start, end);
    }
  };

  resetTime = (): void => {
    // Default range to reset to is one hour ago.
    this.changeDateRange(
      moment.utc().subtract(1, "hours"),
      moment.utc().add(1, "minute"),
    );
  };

  renderTransactionsList(): React.ReactElement {
    return (
      <div className={cx("table-area")}>
        <Loading
          loading={!this.props?.data}
          error={this.props?.error}
          render={() => {
            const {
              data,
              resetSQLStats,
              nodeRegions,
              isTenant,
              onColumnsChange,
              columns: userSelectedColumnsToShow,
              sortSetting,
            } = this.props;
            const { pagination, search, filters } = this.state;
            const { statements, internal_app_name_prefix } = data;
            const appNames = getTrxAppFilterOptions(
              data.transactions,
              internal_app_name_prefix,
            );
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
              searchTransactionsData(search, data.transactions, statements),
              filters,
              internal_app_name_prefix,
              statements,
              nodeRegions,
              isTenant,
            );
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
              this.handleDetails,
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
                  <PageConfigItem>
                    <DateRange
                      start={this.props.dateRange[0]}
                      end={this.props.dateRange[1]}
                      onSubmit={this.changeDateRange}
                    />
                  </PageConfigItem>
                  <PageConfigItem>
                    <button
                      className={cx("reset-btn")}
                      onClick={this.resetTime}
                    >
                      reset time
                    </button>
                  </PageConfigItem>
                  <PageConfigItem className={commonStyles("separator")}>
                    <ClearStats
                      resetSQLStats={resetSQLStats}
                      tooltipType="transaction"
                    />
                  </PageConfigItem>
                </PageConfig>
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
      </div>
    );
  }

  renderTransactionDetails(): React.ReactElement {
    const { statements } = this.props.data;
    const {
      aggregatedTs,
      statementFingerprintIds,
      transactionStats,
      transactionFingerprintId,
    } = this.state;
    const transactionDetails =
      statementFingerprintIds &&
      getStatementsByFingerprintIdAndTime(
        statementFingerprintIds,
        aggregatedTs,
        statements,
      );
    const transactionText =
      statementFingerprintIds &&
      statementFingerprintIdsToText(statementFingerprintIds, statements);

    return (
      <TransactionDetails
        transactionText={transactionText}
        statements={transactionDetails}
        nodeRegions={this.props.nodeRegions}
        transactionStats={transactionStats}
        lastReset={this.lastReset()}
        handleDetails={this.handleDetails}
        error={this.props.error}
        resetSQLStats={this.props.resetSQLStats}
        isTenant={this.props.isTenant}
        transactionFingerprintId={transactionFingerprintId}
      />
    );
  }

  render(): React.ReactElement {
    const { statementFingerprintIds } = this.state;
    const renderTxDetailsView = !!statementFingerprintIds;
    return renderTxDetailsView
      ? this.renderTransactionDetails()
      : this.renderTransactionsList();
  }
}
