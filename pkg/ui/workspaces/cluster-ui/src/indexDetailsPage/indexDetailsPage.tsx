// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Caution, Search as IndexIcon } from "@cockroachlabs/icons";
import { Heading, Icon } from "@cockroachlabs/ui-components";
import { Col, Row, Tooltip } from "antd";
import classNames from "classnames/bind";
import flatMap from "lodash/flatMap";
import moment, { Moment } from "moment-timezone";
import React from "react";

import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import {
  ISortedTablePagination,
  SortedTable,
  SortSetting,
} from "src/sortedtable";
import { SqlBox, SqlBoxSize } from "src/sql";
import { Timestamp } from "src/timestamp";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import { INTERNAL_APP_NAME_PREFIX } from "src/util/constants";

import { Anchor } from "../anchor";
import {
  getStatementsUsingIndex,
  StatementsListRequestFromDetails,
  StatementsUsingIndexRequest,
} from "../api/indexDetailsApi";
import { commonStyles } from "../common";
import { CockroachCloudContext } from "../contexts";
import { Pagination } from "../pagination";
import {
  calculateActiveFilters,
  defaultFilters,
  Filter,
  Filters,
} from "../queryFilter";
import { Search } from "../search";
import Breadcrumbs from "../sharedFromCloud/breadcrumbs";
import LoadingError from "../sqlActivity/errorComponent";
import { filterStatementsData } from "../sqlActivity/util";
import { EmptyStatementsPlaceholder } from "../statementsPage/emptyStatementsPlaceholder";
import { StatementViewType } from "../statementsPage/statementPageTypes";
import statementsStyles from "../statementsPage/statementsPage.module.scss";
import {
  AggregateStatistics,
  makeStatementsColumns,
  populateRegionNodeForStatements,
} from "../statementsTable";
import { UIConfigState } from "../store";
import { SummaryCard } from "../summaryCard";
import { TableStatistics } from "../tableStatistics";
import {
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
} from "../timeScaleDropdown";
import {
  calculateTotalWorkload,
  Count,
  DATE_FORMAT_24_TZ,
  EncodeDatabaseTableIndexUri,
  performanceTuningRecipes,
  unique,
  unset,
} from "../util";
import {
  databaseDetailsPagePath,
  DB_PAGE_PATH,
  tableDetailsPagePath,
} from "../util/routes";

import styles from "./indexDetailsPage.module.scss";

const cx = classNames.bind(styles);
const stmtCx = classNames.bind(statementsStyles);

// We break out separate interfaces for some of the nested objects in our data
// so that we can make (typed) test assertions on narrower slices of the data.
//
// The loading and loaded flags help us know when to dispatch the appropriate
// refresh actions.
//
// The overall structure is:
//
//   interface IndexDetailsPageData {
//     databaseName: string;
//     tableName: string;
//     indexName: string;
//     details: { // IndexDetails;
//       loading: boolean;
//       loaded: boolean;
//       tableID: string;
//       indexID: string;
//       createStatement: string;
//       totalReads: number;
//       lastRead: Moment;
//       lastReset: Moment;
//     }
//   }

export interface IndexDetailsPageData {
  databaseName: string;
  tableName: string;
  indexName: string;
  details: IndexDetails;
  isTenant: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  hasAdminRole?: UIConfigState["hasAdminRole"];
  nodeRegions: { [nodeId: string]: string };
  timeScale: TimeScale;
}

interface IndexDetails {
  loading: boolean;
  loaded: boolean;
  tableID: string;
  indexID: string;
  createStatement: string;
  totalReads: number;
  lastRead: Moment;
  lastReset: Moment;
  indexRecommendations: IndexRecommendation[];
  databaseID: number;
}

export type RecommendationType = "DROP_UNUSED" | "Unknown";

interface IndexRecommendation {
  type: RecommendationType;
  reason: string;
}

export interface IndexDetailPageActions {
  refreshIndexStats?: (database: string, table: string) => void;
  resetIndexUsageStats?: (database: string, table: string) => void;
  refreshNodes?: () => void;
  refreshUserSQLRoles: () => void;
  onTimeScaleChange: (ts: TimeScale) => void;
}

export type IndexDetailsPageProps = IndexDetailsPageData &
  IndexDetailPageActions;

interface IndexDetailsPageState {
  activeFilters: number;
  filters?: Filters;
  search: string;
  lastStatementsError: Error | null;
  lastStatementsUpdated: moment.Moment | null;
  statements: AggregateStatistics[];
  stmtPagination: ISortedTablePagination;
  stmtSortSetting: SortSetting;
}

export class IndexDetailsPage extends React.Component<
  IndexDetailsPageProps,
  IndexDetailsPageState
> {
  static contextType = CockroachCloudContext;

  refreshDataInterval: NodeJS.Timeout;
  constructor(props: IndexDetailsPageProps) {
    super(props);

    this.state = {
      stmtSortSetting: {
        ascending: true,
        columnTitle: "time",
      },
      stmtPagination: {
        pageSize: 10,
        current: 1,
      },
      statements: [],
      lastStatementsUpdated: null,
      lastStatementsError: null,
      search: null,
      activeFilters: 0,
      filters: defaultFilters,
    };
  }

  componentDidMount(): void {
    this.refresh();

    this.refreshDataInterval = setInterval(() => {
      this.refreshStatementsList();
    }, 100);
  }

  componentWillUnmount(): void {
    if (this.refreshDataInterval) {
      clearInterval(this.refreshDataInterval);
    }
  }

  componentDidUpdate(): void {
    this.refresh();
  }

  onChangeSortSetting = (ss: SortSetting): void => {
    this.setState({
      stmtSortSetting: ss,
    });
  };

  onChangePage = (current: number): void => {
    const { stmtPagination } = this.state;
    this.setState({ stmtPagination: { ...stmtPagination, current } });
  };

  resetPagination = (): void => {
    this.setState({
      stmtPagination: {
        current: 1,
        pageSize: 10,
      },
    });
  };

  onSubmitSearchField = (search: string): void => {
    this.setState({ search: search });
    this.resetPagination();
  };

  onClearSearchField = (): void => {
    this.onSubmitSearchField("");
  };

  onSubmitFilters = (filters: Filters): void => {
    this.setState({
      filters: filters,
      activeFilters: calculateActiveFilters(filters),
    });

    this.resetPagination();
  };

  onClearFilters = (): void => {
    this.setState({
      filters: defaultFilters,
      activeFilters: 0,
    });

    this.resetPagination();
  };

  changeTimeScale = (ts: TimeScale): void => {
    if (this.props.onTimeScaleChange) {
      this.props.onTimeScaleChange(ts);
    }
    this.setState({ lastStatementsUpdated: null });
    this.refresh();
  };

  private refresh() {
    this.props.refreshUserSQLRoles();
    if (this.props.refreshNodes != null && !this.props.isTenant) {
      this.props.refreshNodes();
    }
    if (!this.props.details.loaded && !this.props.details.loading) {
      return this.props.refreshIndexStats(
        this.props.databaseName,
        this.props.tableName,
      );
    }
  }

  private refreshStatementsList(): void {
    const noData = this.state.lastStatementsUpdated == null;
    const movingTimeScaleWithPeriodPassed =
      this.props.timeScale.key !== "Custom" &&
      moment().diff(this.state.lastStatementsUpdated, "minutes") >= 5;
    if (
      (noData || movingTimeScaleWithPeriodPassed) &&
      this.props.details.loaded
    ) {
      const req: StatementsUsingIndexRequest = StatementsListRequestFromDetails(
        this.props.details.tableID,
        this.props.details.indexID,
        this.props.databaseName,
        this.props.timeScale,
      );
      getStatementsUsingIndex(req)
        .then(res => {
          populateRegionNodeForStatements(res.results, this.props.nodeRegions);
          this.setState({
            statements: res.results,
            lastStatementsUpdated: moment(),
            lastStatementsError: null,
          });
        })
        .catch(error => {
          this.setState({
            lastStatementsUpdated: moment(),
            lastStatementsError: error,
          });
        });
    }
  }

  private getTimestamp(timestamp: Moment) {
    const minDate = moment.utc("0001-01-01"); // minimum value as per UTC
    if (timestamp.isSame(minDate)) {
      return <>Never</>;
    } else {
      return <Timestamp time={timestamp} format={DATE_FORMAT_24_TZ} />;
    }
  }

  private getApps(): string[] {
    const { statements } = this.state;
    let sawBlank = false;
    let sawInternal = false;
    const apps: { [app: string]: boolean } = {};
    statements.forEach((statement: AggregateStatistics) => {
      if (statement.applicationName.startsWith(INTERNAL_APP_NAME_PREFIX)) {
        sawInternal = true;
      } else if (statement.applicationName) {
        apps[statement.applicationName] = true;
      } else {
        sawBlank = true;
      }
    });
    return []
      .concat(sawInternal ? [INTERNAL_APP_NAME_PREFIX] : [])
      .concat(sawBlank ? [unset] : [])
      .concat(Object.keys(apps).sort());
  }

  private renderIndexRecommendations(
    indexRecommendations: IndexRecommendation[],
  ) {
    if (indexRecommendations.length === 0) {
      return (
        <tr>
          <td>None</td>
        </tr>
      );
    }
    return indexRecommendations.map((recommendation, key) => {
      let recommendationType: string;
      switch (recommendation.type) {
        case "DROP_UNUSED":
          recommendationType = "Drop unused index";
      }
      return (
        <tr key={key} className={cx("index-recommendations-rows")}>
          <td
            className={cx(
              "index-recommendations-rows__header",
              "icon__container",
            )}
          >
            <Caution className={cx("icon--s", "icon--warning")} />
            {recommendationType}
          </td>
          <td
            className={cx(
              "index-recommendations-rows__content",
              "index-recommendations__tooltip-anchor",
            )}
          >
            <span className={cx("summary-card--label")}>Reason:</span>{" "}
            {recommendation.reason}{" "}
            <Anchor href={performanceTuningRecipes} target="_blank">
              Learn more
            </Anchor>
          </td>
        </tr>
      );
    });
  }

  private renderBreadcrumbs() {
    // If no props are passed, render db-console breadcrumb links by default.
    return (
      <Breadcrumbs
        items={[
          { link: DB_PAGE_PATH, name: "Databases" },
          {
            link: databaseDetailsPagePath(this.props.details.databaseID),
            name: this.props.databaseName,
          },
          {
            link: tableDetailsPagePath(
              parseInt(this.props.details.tableID, 10),
            ),
            name: `Table: ${this.props.tableName}`,
          },
          {
            link: EncodeDatabaseTableIndexUri(
              this.props.databaseName,
              this.props.tableName,
              this.props.indexName,
            ),
            name: `Index: ${this.props.indexName}`,
          },
        ]}
        divider={<Icon iconName="CaretRight" size="tiny" />}
        className={cx("header-breadcrumbs")}
      />
    );
  }

  private filteredStatements = (): AggregateStatistics[] => {
    const { filters, search, statements } = this.state;
    const { isTenant } = this.props;
    let filteredStatements = statements;
    const isInternal = (statement: AggregateStatistics) =>
      statement.applicationName.startsWith(INTERNAL_APP_NAME_PREFIX);

    if (filters.app && filters.app !== "All") {
      const criteria = decodeURIComponent(filters.app).split(",");
      let showInternal = false;
      if (criteria.includes(INTERNAL_APP_NAME_PREFIX)) {
        showInternal = true;
      }
      if (criteria.includes(unset)) {
        criteria.push("");
      }

      filteredStatements = statements.filter(
        (statement: AggregateStatistics) =>
          (showInternal && isInternal(statement)) ||
          criteria.includes(statement.applicationName),
      );
    }
    return filterStatementsData(filters, search, filteredStatements, isTenant);
  };

  render(): React.ReactElement {
    const {
      statements,
      stmtSortSetting,
      stmtPagination,
      search,
      activeFilters,
      filters,
    } = this.state;
    const { nodeRegions, isTenant, hasViewActivityRedactedRole, hasAdminRole } =
      this.props;
    const apps = this.getApps();
    const nodes = Object.keys(nodeRegions)
      .map(n => Number(n))
      .sort();
    const regions = unique(
      isTenant
        ? flatMap(statements, statement => statement.stats.regions)
        : nodes.map(node => nodeRegions[node.toString()]),
    ).sort();

    const filteredStmts = this.filteredStatements();

    return (
      <div className={cx("page-container")}>
        <div className="root table-area">
          <section className={baseHeadingClasses.wrapper}>
            {this.renderBreadcrumbs()}
          </section>
          <div className={cx("header-container")}>
            <h3
              className={`${baseHeadingClasses.tableName} ${cx(
                "icon__container",
              )}`}
            >
              <IndexIcon className={cx("icon--md", "icon--title")} />
              {this.props.indexName}
            </h3>
            <div className={cx("reset-info")}>
              <Tooltip
                placement="bottom"
                title="Index stats accumulate from the time the index was created or had its stats reset.. Clicking ‘Reset all index stats’ will reset index stats for the entire cluster. Last reset is the timestamp at which the last reset started."
              >
                <div className={cx("last-reset", "underline")}>
                  Last reset: {this.getTimestamp(this.props.details.lastReset)}
                </div>
              </Tooltip>
              {hasAdminRole && (
                <div>
                  <a
                    className={cx(
                      "action",
                      "separator",
                      "index-stats__reset-btn",
                    )}
                    onClick={() =>
                      this.props.resetIndexUsageStats(
                        this.props.databaseName,
                        this.props.tableName,
                      )
                    }
                  >
                    Reset all index stats
                  </a>
                </div>
              )}
            </div>
          </div>
          <section className={baseHeadingClasses.wrapper}>
            <Row gutter={18}>
              <Col className="gutter-row" span={18}>
                <SqlBox
                  value={this.props.details.createStatement}
                  size={SqlBoxSize.CUSTOM}
                />
              </Col>
            </Row>
            <Row gutter={18}>
              <Col className="gutter-row" span={18}>
                <SummaryCard className={cx("summary-card--row")}>
                  <table className="table">
                    <tbody>
                      <tr className={cx("summary-card--row", "table__row")}>
                        <td
                          className={cx(
                            "table__cell",
                            "summary-card--label-cell",
                          )}
                        >
                          <h4 className={cx("summary-card--label")}>
                            Total Reads
                          </h4>
                        </td>
                        <td className="table__cell">
                          <p className={cx("summary-card--value")}>
                            {Count(this.props.details.totalReads)}
                          </p>
                        </td>
                      </tr>
                      <tr className={cx("summary-card--row", "table__row")}>
                        <td
                          className={cx(
                            "table__cell",
                            "summary-card--label-cell",
                          )}
                        >
                          <h4 className={cx("summary-card--label")}>
                            Last Read
                          </h4>
                        </td>
                        <td className="table__cell">
                          <p className={cx("summary-card--value")}>
                            {this.getTimestamp(this.props.details.lastRead)}
                          </p>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </SummaryCard>
              </Col>
            </Row>
            <Row gutter={18} className={cx("row-spaced")}>
              <Col className="gutter-row" span={18}>
                <SummaryCard className={cx("summary-card--row")}>
                  <Heading type="h5">Index Recommendations</Heading>
                  <table>
                    <tbody>
                      {this.renderIndexRecommendations(
                        this.props.details.indexRecommendations,
                      )}
                    </tbody>
                  </table>
                </SummaryCard>
              </Col>
            </Row>
            <Row gutter={24} className={cx("row-spaced", "bottom-space")}>
              <Col className="gutter-row" span={24}>
                <SummaryCard className={cx("summary-card--row")}>
                  <Heading type="h5">Index Usage</Heading>
                  <PageConfig whiteBkg={true}>
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
                        appNames={apps}
                        regions={regions}
                        nodes={nodes.map(n => "n" + n)}
                        activeFilters={activeFilters}
                        filters={filters}
                        hideTimeLabel={true}
                        showDB={false}
                        showSqlType={true}
                        showScan={true}
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
                  </PageConfig>
                  <Loading
                    loading={statements == null}
                    page="index details"
                    error={this.state.lastStatementsError}
                    renderError={() =>
                      LoadingError({
                        statsType: "statements",
                        error: this.state.lastStatementsError,
                      })
                    }
                  >
                    <TableStatistics
                      pagination={stmtPagination}
                      totalCount={filteredStmts.length}
                      arrayItemName={
                        "most executed statement fingerprints using this index"
                      }
                      activeFilters={activeFilters}
                      onClearFilters={this.onClearFilters}
                    />
                    <SortedTable
                      data={filteredStmts}
                      columns={makeStatementsColumns(
                        statements,
                        [],
                        calculateTotalWorkload(statements),
                        "statement",
                        isTenant,
                        hasViewActivityRedactedRole,
                      ).filter(c => !(isTenant && c.hideIfTenant))}
                      className={stmtCx("statements-table")}
                      tableWrapperClassName={cx("table-scroll")}
                      sortSetting={stmtSortSetting}
                      onChangeSortSetting={this.onChangeSortSetting}
                      pagination={stmtPagination}
                      renderNoResult={
                        <EmptyStatementsPlaceholder
                          isEmptySearchResults={
                            (search?.length > 0 || activeFilters > 0) &&
                            filteredStmts?.length === 0
                          }
                          statementView={StatementViewType.USING_INDEX}
                        />
                      }
                    />
                    <Pagination
                      pageSize={stmtPagination.pageSize}
                      current={stmtPagination.current}
                      total={filteredStmts.length}
                      onChange={this.onChangePage}
                    />
                  </Loading>
                </SummaryCard>
              </Col>
            </Row>
          </section>
        </div>
      </div>
    );
  }
}
