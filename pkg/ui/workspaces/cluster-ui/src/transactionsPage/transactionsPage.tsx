// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import flatMap from "lodash/flatMap";
import isString from "lodash/isString";
import merge from "lodash/merge";
import moment from "moment-timezone";
import React from "react";
import { RouteComponentProps } from "react-router-dom";

import {
  SqlStatsSortType,
  createCombinedStmtsRequest,
  StatementsRequest,
  SqlStatsSortOptions,
  SqlStatsResponse,
} from "src/api/statementsApi";
import { SearchCriteria } from "src/searchCriteria/searchCriteria";
import { TimeScaleLabel } from "src/timeScaleDropdown/timeScaleLabel";
import { Timestamp, TimestampToMoment, syncHistory, unique } from "src/util";
import {
  STATS_LONG_LOADING_DURATION,
  getSortLabel,
  getSortColumn,
  getSubsetWarning,
  getReqSortColumn,
} from "src/util/sqlActivityConstants";

import { RequestState } from "../api";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { isSelectedColumn } from "../columnsSelector/utils";
import { commonStyles } from "../common";
import { Delayed } from "../delayed";
import { Loading } from "../loading";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { PageConfig, PageConfigItem } from "../pageConfig";
import { Pagination, ResultsPerPageLabel } from "../pagination";
import {
  Filter,
  Filters,
  defaultFilters,
  handleFiltersFromQueryString,
  updateFiltersQueryParamsOnTab,
  SelectedFilters,
} from "../queryFilter";
import { Search } from "../search";
import {
  handleSortSettingFromQueryString,
  ISortedTablePagination,
  SortSetting,
  updateSortSettingQueryParamsOnTab,
} from "../sortedtable";
import ClearStats from "../sqlActivity/clearStats";
import LoadingError from "../sqlActivity/errorComponent";
import styles from "../statementsPage/statementsPage.module.scss";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { UIConfigState } from "../store";
import {
  TimeScale,
  timeScale1hMinOptions,
  getValidOption,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import timeScaleStyles from "../timeScaleDropdown/timeScale.module.scss";
import {
  makeTransactionsColumns,
  TransactionInfo,
  TransactionsTable,
} from "../transactionsTable";

import { EmptyTransactionsPlaceholder } from "./emptyTransactionsPlaceholder";
import { statisticsClasses } from "./transactionsPageClasses";
import { TransactionViewType } from "./transactionsPageTypes";
import {
  generateRegion,
  generateRegionNode,
  getTrxAppFilterOptions,
  searchTransactionsData,
  filterTransactions,
} from "./utils";

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
  txnsResp: RequestState<SqlStatsResponse>;
  timeScale: TimeScale;
  limit: number;
  reqSortSetting: SqlStatsSortType;
  filters: Filters;
  isTenant?: UIConfigState["isTenant"];
  nodeRegions: { [nodeId: string]: string };
  search: string;
  sortSetting: SortSetting;
  hasAdminRole?: UIConfigState["hasAdminRole"];
  requestTime: moment.Moment;
  oldestDataAvailable: Timestamp;
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
  onApplySearchCriteria: (ts: TimeScale, limit: number, sort: string) => void;
  onRequestTimeChange: (t: moment.Moment) => void;
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
    if (onSearchComplete && searchQuery && search !== searchQuery) {
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
      !this.props.txnsResp.valid ||
      !this.props.txnsResp.data ||
      !this.props.txnsResp.lastUpdated
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
    if (currentTab === tab && search && search !== searchQueryString) {
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

  isSortSettingSameAsReqSort = (): boolean => {
    return (
      getSortColumn(this.props.reqSortSetting) ===
      this.props.sortSetting.columnTitle
    );
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
    return new Date(
      Number(this.props.txnsResp?.data?.last_reset.seconds) * 1000,
    );
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
    if (this.props.limit !== this.state.limit) {
      this.props.onChangeLimit(this.state.limit);
    }

    if (this.props.reqSortSetting !== this.state.reqSortSetting) {
      this.props.onChangeReqSort(this.state.reqSortSetting);
    }

    if (this.props.timeScale !== this.state.timeScale) {
      this.props.onTimeScaleChange(this.state.timeScale);
    }

    if (this.props.onApplySearchCriteria) {
      this.props.onApplySearchCriteria(
        this.state.timeScale,
        this.state.limit,
        getSortLabel(this.state.reqSortSetting, "Transaction"),
      );
    }
    this.props.onRequestTimeChange(moment());
    this.refreshData();
    const ss: SortSetting = {
      ascending: false,
      columnTitle: getSortColumn(this.state.reqSortSetting),
    };
    this.onChangeSortSetting(ss);
  };

  onUpdateSortSettingAndApply = (): void => {
    this.setState(
      {
        reqSortSetting: getReqSortColumn(this.props.sortSetting.columnTitle),
      },
      () => {
        this.updateRequestParams();
      },
    );
  };

  hasReqSortOption = (): boolean => {
    let found = false;
    Object.values(SqlStatsSortOptions).forEach(option => {
      const optionString = isString(option) ? option : getSortColumn(option);
      if (optionString === this.props.sortSetting.columnTitle) {
        found = true;
      }
    });
    return found;
  };

  renderTransactions(): React.ReactElement {
    const {
      nodeRegions,
      isTenant,
      onColumnsChange,
      columns: userSelectedColumnsToShow,
      sortSetting,
      search,
      hasAdminRole,
    } = this.props;
    const data = this.props.txnsResp.data;
    const { pagination, filters } = this.state;
    const internalAppNamePrefix = data?.internal_app_name_prefix || "";
    const statements = data?.statements || [];

    // We apply the search filters and app name filters prior to aggregating across Node IDs
    // in order to match what's done on the Statements Page.
    //
    // TODO(davidh): Once the redux layer for TransactionsPage is added to this repo,
    // extract this work into the selector
    const { transactions: filteredTransactions, activeFilters } =
      filterTransactions(
        searchTransactionsData(search, data?.transactions || [], statements),
        filters,
        internalAppNamePrefix,
        statements,
        nodeRegions,
        isTenant,
      );

    const appNames = getTrxAppFilterOptions(
      data?.transactions || [],
      internalAppNamePrefix,
    );

    const transactionsToDisplay: TransactionInfo[] = filteredTransactions.map(
      t => ({
        stats_data: t.stats_data,
        regions: generateRegion(t, statements),
        regionNodes: generateRegionNode(t, statements, nodeRegions),
      }),
    );
    const { current, pageSize } = pagination;
    const hasData = data?.transactions?.length > 0;
    const isUsedFilter = search?.length > 0;

    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();

    const regions = unique(
      isTenant
        ? flatMap(statements, statement => statement.stats.regions)
        : nodes.map(node => nodeRegions[node.toString()]),
    ).sort();

    // Creates a list of all possible columns,
    // hiding nodeRegions if is not multi-region and
    // hiding columns that won't be displayed for virtual clusters.
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
          label: getLabel(c.name as StatisticTableColumnKeys, "transaction"),
          value: c.name,
          isSelected: isSelectedColumn(userSelectedColumnsToShow, c),
        }),
      );

    // List of all columns that will be displayed based on the column selection.
    const displayColumns = columns.filter(c =>
      isSelectedColumn(userSelectedColumnsToShow, c),
    );

    const sortSettingLabel = getSortLabel(
      this.props.reqSortSetting,
      "Transaction",
    );
    const showSortWarning =
      !this.isSortSettingSameAsReqSort() &&
      this.hasReqSortOption() &&
      transactionsToDisplay.length === this.props.limit;

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
          </PageConfig>
          <PageConfig className={cx("float-right")}>
            <PageConfigItem>
              <p className={timeScaleStylesCx("time-label")}>
                <TimeScaleLabel
                  timeScale={this.props.timeScale}
                  requestTime={moment(this.props.requestTime)}
                  oldestDataTime={
                    this.props.oldestDataAvailable &&
                    TimestampToMoment(this.props.oldestDataAvailable)
                  }
                />
                {", "}
                <ResultsPerPageLabel
                  pagination={{
                    ...pagination,
                    total: transactionsToDisplay.length,
                  }}
                  pageName={"Transactions"}
                  search={search}
                />
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
                  tooltipType="transaction"
                />
              </PageConfigItem>
            )}
          </PageConfig>
        </section>
        <section className={statisticsClasses.tableContainerClass}>
          <SelectedFilters
            filters={filters}
            onRemoveFilter={this.onSubmitFilters}
            onClearFilters={this.onClearFilters}
          />
          {showSortWarning && (
            <InlineAlert
              intent="warning"
              title={getSubsetWarning(
                "transaction",
                this.props.limit,
                sortSettingLabel,
                this.props.sortSetting.columnTitle as StatisticTableColumnKeys,
                this.onUpdateSortSettingAndApply,
              )}
              className={cx("margin-bottom")}
            />
          )}
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
          searchType="Transaction"
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
            loading={this.props.txnsResp.inFlight}
            page={"transactions"}
            error={this.props.txnsResp?.error}
            render={() => this.renderTransactions()}
            renderError={() =>
              LoadingError({
                statsType: "transactions",
                error: this.props.txnsResp.error,
                sourceTables: this.props.txnsResp?.data?.txns_source_table && [
                  this.props.txnsResp?.data?.txns_source_table,
                  this.props.txnsResp?.data?.stmts_source_table,
                ],
              })
            }
          />
          {this.props.txnsResp.inFlight && longLoadingMessage}
        </div>
      </>
    );
  }
}
