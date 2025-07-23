import React from "react";
import { RouteComponentProps } from "react-router-dom";
import { isNil, merge, forIn } from "lodash";
import Helmet from "react-helmet";
import classNames from "classnames/bind";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { ColumnDescriptor, SortSetting } from "src/sortedtable";
import { Search } from "src/search";
import { Pagination } from "src/pagination";
import { TableStatistics } from "../tableStatistics";
import {
  Filter,
  Filters,
  defaultFilters,
  calculateActiveFilters,
  getFiltersFromQueryString,
  getTimeValueInSeconds,
} from "../queryFilter";

import { appAttr, getMatchParamByName, calculateTotalWorkload } from "src/util";
import {
  AggregateStatistics,
  makeStatementsColumns,
  statementColumnLabels,
  StatementsSortedTable,
  StatementTableColumnKeys,
} from "../statementsTable";
import {
  ActivateStatementDiagnosticsModal,
  ActivateDiagnosticsModalRef,
} from "src/statementsDiagnostics";
import { ISortedTablePagination } from "../sortedtable";
import styles from "./statementsPage.module.scss";
import { EmptyStatementsPlaceholder } from "./emptyStatementsPlaceholder";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

// Most of the props are supposed to be provided as connected props
// from redux store.
// StatementsPageDispatchProps, StatementsPageStateProps, and StatementsPageOuterProps interfaces
// provide convenient definitions for `mapDispatchToProps`, `mapStateToProps` and props that
// have to be provided by parent component.
export interface StatementsPageDispatchProps {
  refreshStatements: () => void;
  refreshStatementDiagnosticsRequests: () => void;
  resetSQLStats: () => void;
  dismissAlertMessage: () => void;
  onActivateStatementDiagnostics: (statement: string) => void;
  onDiagnosticsModalOpen?: (statement: string) => void;
  onSearchComplete?: (results: AggregateStatistics[]) => void;
  onPageChanged?: (newPage: number) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onDiagnosticsReportDownload?: (report: IStatementDiagnosticsReport) => void;
  onFilterChange?: (value: string) => void;
  onStatementClick?: (statement: string) => void;
  onColumnsChange?: (selectedColumns: string[]) => void;
}

export interface StatementsPageStateProps {
  statements: AggregateStatistics[];
  statementsError: Error | null;
  apps: string[];
  databases: string[];
  totalFingerprints: number;
  lastReset: string;
  columns: string[];
  nodeRegions: { [key: string]: string };
}

export interface StatementsPageState {
  sortSetting: SortSetting;
  search?: string;
  pagination: ISortedTablePagination;
  filters?: Filters;
  activeFilters?: number;
}

export type StatementsPageProps = StatementsPageDispatchProps &
  StatementsPageStateProps &
  RouteComponentProps<unknown>;

export class StatementsPage extends React.Component<
  StatementsPageProps,
  StatementsPageState
> {
  activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>;
  constructor(props: StatementsPageProps) {
    super(props);
    const filters = getFiltersFromQueryString(
      this.props.history.location.search,
    );
    const defaultState = {
      sortSetting: {
        // Sort by Execution Count column as default option.
        ascending: false,
        columnTitle: "executionCount",
      },
      pagination: {
        pageSize: 20,
        current: 1,
      },
      search: "",
      filters: filters,
      activeFilters: calculateActiveFilters(filters),
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
    this.activateDiagnosticsRef = React.createRef();
  }

  getStateFromHistory = (): Partial<StatementsPageState> => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const ascending = searchParams.get("ascending") || undefined;
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const searchQuery = searchParams.get("q") || undefined;

    return {
      sortSetting: {
        ascending: ascending === "true",
        columnTitle,
      },
      search: searchQuery,
    };
  };

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

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });

    this.syncHistory({
      ascending: ss.ascending.toString(),
      columnTitle: ss.columnTitle,
    });
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Statements", ss.columnTitle, ss.ascending);
    }
  };

  selectApp = (value: string) => {
    if (value == "All") value = "";
    const { history, onFilterChange } = this.props;
    history.location.pathname = `/statements/${encodeURIComponent(value)}`;
    history.replace(history.location);
    this.resetPagination();
    if (onFilterChange) {
      onFilterChange(value);
    }
  };

  resetPagination = () => {
    this.setState(prevState => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  componentDidMount() {
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
  }

  componentDidUpdate = (
    __: StatementsPageProps,
    prevState: StatementsPageState,
  ) => {
    if (this.state.search && this.state.search !== prevState.search) {
      this.props.onSearchComplete(this.filteredStatementsData());
    }
    this.props.refreshStatements();
    this.props.refreshStatementDiagnosticsRequests();
  };

  componentWillUnmount() {
    this.props.dismissAlertMessage();
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    this.props.onPageChanged(current);
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
      activeFilters: calculateActiveFilters(filters),
    });

    this.resetPagination();
    this.syncHistory({
      app: filters.app,
      timeNumber: filters.timeNumber,
      timeUnit: filters.timeUnit,
      sqlType: filters.sqlType,
      database: filters.database,
    });
    this.selectApp(filters.app);
  };

  onClearSearchField = () => {
    this.setState({ search: "" });
    this.syncHistory({
      q: undefined,
    });
  };

  onClearFilters = () => {
    this.setState({
      filters: {
        ...defaultFilters,
      },
      activeFilters: 0,
    });
    this.resetPagination();
    this.syncHistory({
      app: undefined,
      timeNumber: undefined,
      timeUnit: undefined,
      sqlType: undefined,
      database: undefined,
    });
    this.selectApp("");
  };

  filteredStatementsData = () => {
    const { search, filters } = this.state;
    const { statements } = this.props;
    const timeValue = getTimeValueInSeconds(filters);
    const sqlTypes =
      filters.sqlType.length > 0
        ? filters.sqlType.split(",").map(function(sqlType: string) {
            // Adding "Type" to match the value on the Statement
            // Possible values: TypeDDL, TypeDML, TypeDCL and TypeTCL
            return "Type" + sqlType;
          })
        : [];
    const databases =
      filters.database.length > 0 ? filters.database.split(",") : [];

    return statements
      .filter(
        statement =>
          databases.length == 0 || databases.includes(statement.database),
      )
      .filter(statement =>
        this.state.filters.fullScan ? statement.fullScan : true,
      )
      .filter(statement =>
        search
          .split(" ")
          .every(val =>
            statement.label.toLowerCase().includes(val.toLowerCase()),
          ),
      )
      .filter(
        statement =>
          statement.stats.service_lat.mean >= timeValue ||
          timeValue === "empty",
      )
      .filter(
        statement =>
          sqlTypes.length == 0 || sqlTypes.includes(statement.stats.sql_type),
      );
  };

  renderStatements = () => {
    const { pagination, search, filters, activeFilters } = this.state;
    const {
      statements,
      databases,
      match,
      lastReset,
      onDiagnosticsReportDownload,
      onStatementClick,
      resetSQLStats,
      columns: userSelectedColumnsToShow,
      onColumnsChange,
    } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const appOptions = [{ value: "All", label: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, label: app }));
    const data = this.filteredStatementsData();
    const totalWorkload = calculateTotalWorkload(data);
    const totalCount = data.length;
    const isEmptySearchResults = statements?.length > 0 && search?.length > 0;

    // Creates a list of all possible columns
    const columns = makeStatementsColumns(
      statements,
      selectedApp,
      totalWorkload,
      search,
      this.activateDiagnosticsRef,
      onDiagnosticsReportDownload,
      onStatementClick,
    );

    const isColumnSelected = (c: ColumnDescriptor<AggregateStatistics>) => {
      return (
        (userSelectedColumnsToShow === null && c.showByDefault !== false) || // show column if list of visible was never defined and can be show by default.
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
          label: statementColumnLabels[c.name as StatementTableColumnKeys],
          value: c.name,
          isSelected: isColumnSelected(c),
        }),
      );

    // List of all columns that will be displayed based on the column selection.
    const displayColumns = columns.filter(c => isColumnSelected(c));

    return (
      <div>
        <PageConfig>
          <PageConfigItem>
            <Search
              onSubmit={this.onSubmitSearchField as any}
              onClear={this.onClearSearchField}
              defaultValue={search}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Filter
              onSubmitFilters={this.onSubmitFilters}
              appNames={appOptions}
              dbNames={databases}
              activeFilters={activeFilters}
              filters={filters}
              showDB={true}
              showSqlType={true}
              showScan={true}
            />
          </PageConfigItem>
        </PageConfig>
        <section className={sortableTableCx("cl-table-container")}>
          <div>
            <ColumnsSelector
              options={tableColumns}
              onSubmitColumns={onColumnsChange}
            />
            <TableStatistics
              pagination={pagination}
              lastReset={lastReset}
              search={search}
              totalCount={totalCount}
              arrayItemName="statements"
              activeFilters={activeFilters}
              onClearFilters={this.onClearFilters}
              resetSQLStats={resetSQLStats}
            />
          </div>
          <StatementsSortedTable
            className="statements-table"
            data={data}
            columns={displayColumns}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            renderNoResult={
              <EmptyStatementsPlaceholder
                isEmptySearchResults={isEmptySearchResults}
              />
            }
            pagination={pagination}
          />
        </section>
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={data.length}
          onChange={this.onChangePage}
        />
      </div>
    );
  };

  render() {
    const {
      match,
      refreshStatementDiagnosticsRequests,
      onActivateStatementDiagnostics,
      onDiagnosticsModalOpen,
    } = this.props;
    const app = getMatchParamByName(match, appAttr);
    return (
      <div className={cx("root", "table-area")}>
        <Helmet title={app ? `${app} App | Statements` : "Statements"} />

        <section className={cx("section")}>
          <h1 className={cx("base-heading")}>Statements</h1>
        </section>

        <Loading
          loading={isNil(this.props.statements)}
          error={this.props.statementsError}
          render={this.renderStatements}
        />
        <ActivateStatementDiagnosticsModal
          ref={this.activateDiagnosticsRef}
          activate={onActivateStatementDiagnostics}
          refreshDiagnosticsReports={refreshStatementDiagnosticsRequests}
          onOpenModal={onDiagnosticsModalOpen}
        />
      </div>
    );
  }
}
