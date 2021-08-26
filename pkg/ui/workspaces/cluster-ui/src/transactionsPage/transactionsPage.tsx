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
import { TransactionInfo, TransactionsTable } from "../transactionsTable";
import { TransactionDetails } from "../transactionDetails";
import { ISortedTablePagination, SortSetting } from "../sortedtable";
import { Pagination } from "../pagination";
import { TableStatistics } from "../tableStatistics";
import {
  baseHeadingClasses,
  statisticsClasses,
} from "./transactionsPageClasses";
import {
  aggregateAcrossNodeIDs,
  generateRegionNode,
  getTrxAppFilterOptions,
  statementFingerprintIdsToText,
} from "./utils";
import {
  searchTransactionsData,
  filterTransactions,
  getStatementsByFingerprintId,
} from "./utils";
import { forIn } from "lodash";
import Long from "long";
import { getSearchParams, unique } from "src/util";
import { EmptyTransactionsPlaceholder } from "./emptyTransactionsPlaceholder";
import { Loading } from "../loading";
import { PageConfig, PageConfigItem } from "../pageConfig";
import { Search } from "../search";
import {
  Filter,
  Filters,
  defaultFilters,
  getFiltersFromQueryString,
} from "../queryFilter";
import { UIConfigState } from "../store/uiConfig";

type IStatementsResponse = protos.cockroach.server.serverpb.IStatementsResponse;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

const cx = classNames.bind(styles);

interface TState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
  search?: string;
  filters?: Filters;
  statementFingerprintIds: Long[] | null;
  transactionStats: TransactionStats | null;
}

export interface TransactionsPageStateProps {
  data: IStatementsResponse;
  nodeRegions: { [nodeId: string]: string };
  error?: Error | null;
  pageSize?: number;
  isTenant?: UIConfigState["isTenant"];
}

export interface TransactionsPageDispatchProps {
  refreshData: () => void;
  resetSQLStats: () => void;
}

export type TransactionsPageProps = TransactionsPageStateProps &
  TransactionsPageDispatchProps &
  RouteComponentProps;

export class TransactionsPage extends React.Component<
  TransactionsPageProps,
  TState
> {
  static defaultProps: Partial<TransactionsPageProps> = {
    isTenant: false,
  };

  trxSearchParams = getSearchParams(this.props.history.location.search);
  filters = getFiltersFromQueryString(this.props.history.location.search);
  state: TState = {
    sortSetting: {
      // Sort by Execution Count column as default option.
      ascending: this.trxSearchParams("ascending", false).toString() === "true",
      columnTitle: this.trxSearchParams(
        "columnTitle",
        "execution count",
      ).toString(),
    },
    pagination: {
      pageSize: this.props.pageSize || 20,
      current: 1,
    },
    search: this.trxSearchParams("q", "").toString(),
    filters: this.filters,
    statementFingerprintIds: null,
    transactionStats: null,
  };

  componentDidMount() {
    this.props.refreshData();
  }
  componentDidUpdate() {
    this.props.refreshData();
  }

  syncHistory = (params: Record<string, string | undefined>) => {
    const { history } = this.props;
    const currentSearchParams = new URLSearchParams(history.location.search);

    forIn(params, (value, key) => {
      if (!value) {
        currentSearchParams.delete(key);
      } else {
        currentSearchParams.set(key, value);
      }
    });

    history.location.search = currentSearchParams.toString();
    history.replace(history.location);
  };

  onChangeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
    this.syncHistory({
      ascending: ss.ascending.toString(),
      columnTitle: ss.columnTitle,
    });
  };

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  resetPagination = () => {
    this.setState((prevState: TState) => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  onClearSearchField = () => {
    this.setState({ search: "" });
    this.syncHistory({
      q: undefined,
    });
  };

  onSubmitSearchField = (search: string) => {
    this.setState({ search });
    this.resetPagination();
    this.syncHistory({
      q: search,
    });
  };

  onSubmitFilters = (filters: Filters) => {
    this.setState({
      filters: {
        ...this.state.filters,
        ...filters,
      },
    });
    this.resetPagination();
    this.syncHistory({
      app: filters.app,
      timeNumber: filters.timeNumber,
      timeUnit: filters.timeUnit,
      regions: filters.regions,
      nodes: filters.nodes,
    });
  };

  onClearFilters = () => {
    this.setState({
      filters: {
        ...defaultFilters,
      },
    });
    this.resetPagination();
    this.syncHistory({
      app: undefined,
      timeNumber: undefined,
      timeUnit: undefined,
      regions: undefined,
      nodes: undefined,
    });
  };

  handleDetails = (
    statementFingerprintIds: Long[] | null,
    transactionStats: TransactionStats,
  ) => {
    this.setState({ statementFingerprintIds, transactionStats });
  };

  lastReset = () => {
    return new Date(Number(this.props.data?.last_reset.seconds) * 1000);
  };

  renderTransactionsList() {
    return (
      <div className={cx("table-area")}>
        <section className={baseHeadingClasses.wrapper}>
          <h1 className={baseHeadingClasses.tableName}>Transactions</h1>
        </section>
        <Loading
          loading={!this.props?.data}
          error={this.props?.error}
          render={() => {
            const { data, resetSQLStats, nodeRegions, isTenant } = this.props;
            const { pagination, search, filters } = this.state;
            const { statements, internal_app_name_prefix } = data;
            const appNames = getTrxAppFilterOptions(
              data.transactions,
              internal_app_name_prefix,
            );
            // If the cluster is a tenant cluster we don't show info
            // about nodes/regions
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
                      showRegions={regions.length > 1 && !isTenant}
                      showNodes={nodes.length > 1 && !isTenant}
                    />
                  </PageConfigItem>
                </PageConfig>
                <section className={statisticsClasses.tableContainerClass}>
                  <TableStatistics
                    pagination={pagination}
                    lastReset={this.lastReset()}
                    search={search}
                    totalCount={transactionsToDisplay.length}
                    arrayItemName="transactions"
                    activeFilters={activeFilters}
                    onClearFilters={this.onClearFilters}
                    resetSQLStats={resetSQLStats}
                  />
                  <TransactionsTable
                    transactions={transactionsToDisplay}
                    statements={statements}
                    nodeRegions={nodeRegions}
                    isTenant={isTenant}
                    sortSetting={this.state.sortSetting}
                    onChangeSortSetting={this.onChangeSortSetting}
                    handleDetails={this.handleDetails}
                    search={search}
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
        />
      </div>
    );
  }

  renderTransactionDetails() {
    const { statements } = this.props.data;
    const { statementFingerprintIds } = this.state;
    const transactionDetails =
      statementFingerprintIds &&
      getStatementsByFingerprintId(statementFingerprintIds, statements);
    const transactionText =
      statementFingerprintIds &&
      statementFingerprintIdsToText(statementFingerprintIds, statements);

    return (
      <TransactionDetails
        transactionText={transactionText}
        statements={transactionDetails}
        nodeRegions={this.props.nodeRegions}
        transactionStats={this.state.transactionStats}
        lastReset={this.lastReset()}
        handleDetails={this.handleDetails}
        error={this.props.error}
        resetSQLStats={this.props.resetSQLStats}
      />
    );
  }

  render() {
    const { statementFingerprintIds } = this.state;
    const renderTxDetailsView = !!statementFingerprintIds;
    return renderTxDetailsView
      ? this.renderTransactionDetails()
      : this.renderTransactionsList();
  }
}
