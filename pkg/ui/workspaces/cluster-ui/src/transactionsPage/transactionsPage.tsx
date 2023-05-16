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
import { statisticsClasses } from "./transactionsPageClasses";
import {
  aggregateAcrossNodeIDs,
  generateRegionNode,
  getTrxAppFilterOptions,
  searchTransactionsData,
  filterTransactions,
} from "./utils";
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
import {
  SqlStatsSortType,
  createCombinedStmtsRequest,
  StatementsRequest,
} from "src/api/statementsApi";
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
  TimeScale,
  timeScaleToString,
  timeScale1hMinOptions,
  getValidOption,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import { InlineAlert } from "@cockroachlabs/ui-components";
import moment from "moment";
import {
  STATS_LONG_LOADING_DURATION,
  txnRequestSortOptions,
  getSortLabel,
} from "src/util/sqlActivityConstants";
import { Button } from "src/button";
import { SearchCriteria } from "src/searchCriteria/searchCriteria";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";

type IStatementsResponse = protos.cockroach.server.serverpb.IStatementsResponse;

const cx = classNames.bind(styles);
const timeScaleStylesCx = classNames.bind(timeScaleStyles);

interface TState {
  filters?: Filters;
  pagination: ISortedTablePagination;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
}

export interface TransactionsPageStateProps {
  columns: string[];
  data: IStatementsResponse;
  isDataValid: boolean;
  isReqInFlight: boolean;
  lastUpdated: moment.Moment | null;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  error?: Error | null;
  filters: Filters;
  isTenant?: UIConfigState["isTenant"];
  nodeRegions: { [nodeId: string]: string };
  search: string;
  sortSetting: SortSetting;
  hasAdminRole?: UIConfigState["hasAdminRole"];
}

export interface TransactionsPageDispatchProps {
  refreshData: (req: StatementsRequest) => void;
  refreshNodes: () => void;
  refreshUserSQLRoles: () => void;
  resetSQLStats: () => void;
  onTimeScaleChange?: (ts: TimeScale) => void;
  onChangeLimit: (limit: number) => void;
  onChangeReqSort: (sort: SqlStatsSortType) => void;
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

type RequestParams = Pick<TState, "timeScale" | "limit" | "reqSortSetting">;

function stmtsRequestFromParams(params: RequestParams): StatementsRequest {
  const [start, end] = toRoundedDateRange(params.timeScale);
  return createCombinedStmtsRequest({
    start,
    end,
    limit: params.limit,
    sort: params.reqSortSetting,
  });
}
export class TransactionsPage extends React.Component<
  TransactionsPageProps,
  TState
> {
  constructor(props: TransactionsPageProps) {
    super(props);

    this.state = {
      limit: this.props.limit,
      timeScale: this.props.timeScale,
      reqSortSetting: this.props.reqSortSetting,
      pagination: {
        pageSize: 50,
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
    const req = stmtsRequestFromParams(this.state);
    this.props.refreshData(req);
  };

  resetSQLStats = (): void => {
    this.props.resetSQLStats();
  };

  componentDidMount(): void {
    // In case the user selected a option not available on this page,
    // force a selection of a valid option. This is necessary for the case
    // where the value 10/30 min is selected on the Metrics page.
    const ts = getValidOption(this.props.timeScale, timeScale1hMinOptions);
    if (ts !== this.props.timeScale) {
      this.changeTimeScale(ts);
    } else if (
      !this.props.isDataValid ||
      !this.props.data ||
      !this.props.lastUpdated
    ) {
      this.refreshData();
    }

    if (!this.props.isTenant) {
      this.props.refreshNodes();
    }

    this.props.refreshUserSQLRoles();
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
    if (!this.props.isTenant) {
      this.props.refreshNodes();
    }
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
    this.setState(prevState => ({ ...prevState, timeScale: ts }));
  };

  onChangeLimit = (newLimit: number): void => {
    this.setState(prevState => ({ ...prevState, limit: newLimit }));
  };

  onChangeReqSort = (newSort: SqlStatsSortType): void => {
    this.setState(prevState => ({ ...prevState, reqSortSetting: newSort }));
  };

  updateRequestParams = (): void => {
    this.props.onChangeLimit(this.state.limit);
    this.props.onChangeReqSort(this.state.reqSortSetting);
    this.props.onTimeScaleChange(this.state.timeScale);
    this.refreshData();
  };

  renderTransactions(): React.ReactElement {
    const {
      data,
      nodeRegions,
      isTenant,
      onColumnsChange,
      columns: userSelectedColumnsToShow,
      sortSetting,
      search,
      hasAdminRole,
    } = this.props;
    const { pagination, filters } = this.state;
    const internal_app_name_prefix = data?.internal_app_name_prefix || "";
    const statements = data?.statements || [];

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
    const hasData = data?.transactions?.length > 0;
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
          label: getLabel(c.name as StatisticTableColumnKeys, "transaction"),
          value: c.name,
          isSelected: isColumnSelected(c),
        }),
      );
    // List of all columns that will be displayed based on the column selection.
    const displayColumns = columns.filter(c => isColumnSelected(c));

    const period = timeScaleToString(this.props.timeScale);

    const clearFilter = activeFilters ? (
      <PageConfigItem>
        <Button onClick={this.onClearFilters} type="flat" size="small">
          clear filter
        </Button>
      </PageConfigItem>
    ) : (
      <></>
    );

    const sortSettingLabel = getSortLabel(this.props.reqSortSetting);
    return (
      <>
        <h5 className={`${commonStyles("base-heading")} ${cx("margin-top")}`}>
          {`Results - Top ${this.props.limit} Transaction Fingerprints by ${sortSettingLabel}`}
        </h5>
        <section className={cx("filter-area")}>
          <PageConfig className={cx("float-left")}>
            <PageConfigItem>
              <Search
                onSubmit={this.onSubmitSearchField}
                onClear={this.onClearSearchField}
                defaultValue={search}
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
            <PageConfigItem>
              <ColumnsSelector
                options={tableColumns}
                onSubmitColumns={onColumnsChange}
              />
            </PageConfigItem>
            {clearFilter}
          </PageConfig>
          <PageConfig className={cx("float-right")}>
            <PageConfigItem>
              <p className={timeScaleStylesCx("time-label")}>
                Showing aggregated stats from{" "}
                <span className={timeScaleStylesCx("bold")}>{period}</span>
              </p>
            </PageConfigItem>
            {hasAdminRole && (
              <PageConfigItem
                className={`${commonStyles("separator")} ${cx(
                  "reset-btn-area",
                )} `}
              >
                <ClearStats
                  resetSQLStats={this.resetSQLStats}
                  tooltipType="statement"
                />
              </PageConfigItem>
            )}
          </PageConfig>
        </section>
        <section className={statisticsClasses.tableContainerClass}>
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
  }

  render(): React.ReactElement {
    const longLoadingMessage = (
      <Delayed delay={STATS_LONG_LOADING_DURATION}>
        <InlineAlert
          intent="info"
          title="If the selected time interval contains a large amount of data, this page might take a few minutes to load."
        />
      </Delayed>
    );

    return (
      <>
        <SearchCriteria
          sortOptions={txnRequestSortOptions}
          topValue={this.state.limit}
          byValue={this.state.reqSortSetting}
          currentScale={this.state.timeScale}
          onChangeTop={this.onChangeLimit}
          onChangeBy={this.onChangeReqSort}
          onChangeTimeScale={this.changeTimeScale}
          onApply={this.updateRequestParams}
        />
        <div className={cx("table-area")}>
          <Loading
            loading={this.props.isReqInFlight}
            page={"transactions"}
            error={this.props?.error}
            render={() => this.renderTransactions()}
            renderError={() =>
              LoadingError({
                statsType: "transactions",
                timeout: this.props?.error?.name
                  ?.toLowerCase()
                  .includes("timeout"),
              })
            }
          />
          {this.props.isReqInFlight && longLoadingMessage}
        </div>
      </>
    );
  }
}
