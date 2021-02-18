import React from "react";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { RouteComponentProps } from "react-router-dom";
import { TransactionsTable } from "../transactionsTable";
import { TransactionDetails } from "../transactionDetails";
import { ISortedTablePagination, SortSetting } from "../sortedtable";
import { Pagination } from "../pagination";
import { TransactionsPageStatistic } from "./transactionsPageStatistic";
import {
  baseHeadingClasses,
  statisticsClasses,
} from "./transactionsPageClasses";
import { aggregateAcrossNodeIDs, getTrxAppFilterOptions } from "./utils";
import {
  searchTransactionsData,
  filterTransactions,
  getStatementsById,
} from "./utils";
import { forIn } from "lodash";
import Long from "long";
import { getSearchParams } from "src/util";
import { EmptyTransactionsPlaceholder } from "./emptyTransactionsPlaceholder";
import { Loading } from "../loading";
import { PageConfig, PageConfigItem } from "../pageConfig";
import { Search } from "../search";
import { Filter } from "./filter";

type IStatementsResponse = protos.cockroach.server.serverpb.IStatementsResponse;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

export interface Filters {
  app?: string;
  transactionsType?: string;
  timeNumber?: string;
  timeUnit?: string;
  fullScans?: boolean;
  distributed?: boolean;
}

const defaultFilters = {
  app: "All",
  timeNumber: "0",
  timeUnit: "seconds",
};

interface TState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
  search?: string;
  filters?: Filters;
  statementIds: Long[] | null;
  transactionStats: TransactionStats | null;
}

interface TransactionsPageProps {
  data: IStatementsResponse;
  refreshData: () => void;
  error?: Error | null;
  pageSize?: number;
}

export class TransactionsPage extends React.Component<
  RouteComponentProps & TransactionsPageProps
> {
  trxSearchParams = getSearchParams(this.props.history.location.search);

  state: TState = {
    sortSetting: {
      sortKey: 3,
      ascending: false,
    },
    pagination: {
      pageSize: this.props.pageSize || 20,
      current: 1,
    },
    search: this.trxSearchParams("q", ""),
    filters: {
      app: this.trxSearchParams("app", defaultFilters.app),
      timeNumber: this.trxSearchParams("timeNumber", defaultFilters.timeNumber),
      timeUnit: this.trxSearchParams("timeUnit", defaultFilters.timeUnit),
    },
    statementIds: null,
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
      sortKey: ss.sortKey,
      ascending: Boolean(ss.ascending).toString(),
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
    });
  };

  handleDetails = (
    statementIds: Long[] | null,
    transactionStats: TransactionStats,
  ) => {
    this.setState({ statementIds, transactionStats });
  };

  lastReset = () => {
    return new Date(Number(this.props.data?.last_reset.seconds) * 1000);
  };

  renderTransactionsList() {
    return (
      <div>
        <section className={baseHeadingClasses.wrapper}>
          <h1 className={baseHeadingClasses.tableName}>Transactions</h1>
        </section>
        <Loading
          loading={!this.props?.data}
          error={this.props?.error}
          render={() => {
            const { data } = this.props;
            const { pagination, search, filters } = this.state;
            const { statements, internal_app_name_prefix } = data;
            const appNames = getTrxAppFilterOptions(
              data.transactions,
              internal_app_name_prefix,
            );
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
            );
            const transactionsToDisplay = aggregateAcrossNodeIDs(
              filteredTransactions,
              statements,
            );
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
                      placeholder={"Search transactions"}
                    />
                  </PageConfigItem>
                  <PageConfigItem>
                    <Filter
                      onSubmitFilters={this.onSubmitFilters}
                      appNames={appNames}
                      activeFilters={activeFilters}
                      filters={filters}
                    />
                  </PageConfigItem>
                </PageConfig>
                <section className={statisticsClasses.tableContainerClass}>
                  <TransactionsPageStatistic
                    pagination={pagination}
                    lastReset={this.lastReset()}
                    search={search}
                    totalCount={transactionsToDisplay.length}
                    arrayItemName="transactions"
                    activeFilters={activeFilters}
                    onClearFilters={this.onClearFilters}
                  />
                  <TransactionsTable
                    transactions={transactionsToDisplay}
                    statements={statements}
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
    const { statementIds } = this.state;
    const transactionDetails =
      statementIds && getStatementsById(statementIds, statements);

    return (
      <TransactionDetails
        statements={transactionDetails}
        transactionStats={this.state.transactionStats}
        lastReset={this.lastReset()}
        handleDetails={this.handleDetails}
        error={this.props.error}
      />
    );
  }

  render() {
    const { statementIds } = this.state;
    const renderTxDetailsView = !!statementIds;
    return renderTxDetailsView
      ? this.renderTransactionDetails()
      : this.renderTransactionsList();
  }
}
