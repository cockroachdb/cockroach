import React from "react";
import { RouteComponentProps } from "react-router-dom";
import { isNil, merge, forIn } from "lodash";
import moment from "moment";
import Helmet from "react-helmet";
import classNames from "classnames/bind";

import { Text } from "@cockroachlabs/ui-components";

import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SortSetting } from "src/sortedtable";
import { Search } from "src/search";
import { Pagination, ResultsPerPageLabel } from "src/pagination";

import { DATE_FORMAT, appAttr, getMatchParamByName } from "src/util";
import {
  AggregateStatistics,
  makeStatementsColumns,
  StatementsSortedTable,
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
}

export interface StatementsPageStateProps {
  statements: AggregateStatistics[];
  statementsError: Error | null;
  apps: string[];
  totalFingerprints: number;
  lastReset: string;
}

export interface StatementsPageState {
  sortSetting: SortSetting;
  search?: string;
  pagination: ISortedTablePagination;
}

export type StatementsPageProps = StatementsPageDispatchProps &
  StatementsPageStateProps &
  RouteComponentProps<any>;

export class StatementsPage extends React.Component<
  StatementsPageProps,
  StatementsPageState
> {
  activateDiagnosticsRef: React.RefObject<ActivateDiagnosticsModalRef>;

  constructor(props: StatementsPageProps) {
    super(props);
    const defaultState = {
      sortSetting: {
        sortKey: 3, // Sort by Execution Count column as default option
        ascending: false,
      },
      pagination: {
        pageSize: 20,
        current: 1,
      },
      search: "",
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
    this.activateDiagnosticsRef = React.createRef();
  }

  getStateFromHistory = (): Partial<StatementsPageState> => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const sortKey = searchParams.get("sortKey") || undefined;
    const ascending = searchParams.get("ascending") || undefined;
    const searchQuery = searchParams.get("q") || undefined;

    return {
      sortSetting: {
        sortKey,
        ascending: Boolean(ascending),
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
      sortKey: ss.sortKey,
      ascending: Boolean(ss.ascending).toString(),
    });
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Statements", ss.columnTitle, ss.ascending);
    }
  };

  selectApp = (value: string) => {
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

  onClearSearchField = () => {
    this.setState({ search: "" });
    this.syncHistory({
      q: undefined,
    });
  };

  filteredStatementsData = () => {
    const { search } = this.state;
    const { statements } = this.props;
    return statements.filter(statement =>
      search
        .split(" ")
        .every(val =>
          statement.label.toLowerCase().includes(val.toLowerCase()),
        ),
    );
  };

  renderLastCleared = () => {
    const { lastReset } = this.props;
    return `Last cleared ${moment.utc(lastReset).format(DATE_FORMAT)}`;
  };

  renderStatements = () => {
    const { pagination, search } = this.state;
    const {
      statements,
      match,
      onDiagnosticsReportDownload,
      onStatementClick,
    } = this.props;
    const appAttrValue = getMatchParamByName(match, appAttr);
    const selectedApp = appAttrValue || "";
    const appOptions = [{ value: "", name: "All" }];
    this.props.apps.forEach(app => appOptions.push({ value: app, name: app }));
    const currentOption = appOptions.find(o => o.value === selectedApp);
    const data = this.filteredStatementsData();
    const totalCount = data.length;
    const isEmptySearchResults = statements?.length > 0 && search?.length > 0;

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
            <Dropdown
              items={appOptions}
              onChange={this.selectApp}
              itemsClassname={cx("app-filter-dropdown-item")}
            >
              <Text
                type="body-strong"
                noWrap={true}
                className={cx("app-filter-dropdown")}
              >
                {`App: ${decodeURIComponent(currentOption.name)}`}
              </Text>
            </Dropdown>
          </PageConfigItem>
        </PageConfig>
        <section className={sortableTableCx("cl-table-container")}>
          <div className={cx("cl-table-statistic")}>
            <h4 className={cx("cl-count-title")}>
              <ResultsPerPageLabel
                pagination={{ ...pagination, total: totalCount }}
                pageName={"statements"}
                selectedApp={selectedApp}
                search={search}
              />
            </h4>
            <h4 className={cx("last-cleared-title")}>
              {this.renderLastCleared()}
            </h4>
          </div>
          <StatementsSortedTable
            className="statements-table"
            data={data}
            columns={makeStatementsColumns(
              statements,
              selectedApp,
              search,
              this.activateDiagnosticsRef,
              onDiagnosticsReportDownload,
              onStatementClick,
            )}
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
      <div className={cx("root")}>
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
