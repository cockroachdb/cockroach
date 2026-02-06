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
import React, { useState, useEffect, useCallback, useRef } from "react";

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

export function IndexDetailsPage(
  props: IndexDetailsPageProps,
): React.ReactElement {
  const {
    databaseName,
    tableName,
    indexName,
    details,
    isTenant,
    hasViewActivityRedactedRole,
    hasAdminRole,
    nodeRegions,
    timeScale,
    refreshIndexStats,
    resetIndexUsageStats,
    refreshNodes,
    refreshUserSQLRoles,
    onTimeScaleChange,
  } = props;

  const [stmtSortSetting, setStmtSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "time",
  });
  const [stmtPagination, setStmtPagination] = useState<ISortedTablePagination>({
    pageSize: 10,
    current: 1,
  });
  const [statements, setStatements] = useState<AggregateStatistics[]>([]);
  const [lastStatementsUpdated, setLastStatementsUpdated] =
    useState<moment.Moment | null>(null);
  const [lastStatementsError, setLastStatementsError] = useState<Error | null>(
    null,
  );
  const [search, setSearch] = useState<string>(null);
  const [activeFilters, setActiveFilters] = useState<number>(0);
  const [filters, setFilters] = useState<Filters>(defaultFilters);

  const refreshDataIntervalRef = useRef<NodeJS.Timeout>(null);

  const refresh = useCallback(() => {
    refreshUserSQLRoles();
    if (refreshNodes != null && !isTenant) {
      refreshNodes();
    }
    if (!details.loaded && !details.loading) {
      refreshIndexStats?.(databaseName, tableName);
    }
  }, [
    refreshUserSQLRoles,
    refreshNodes,
    isTenant,
    details.loaded,
    details.loading,
    refreshIndexStats,
    databaseName,
    tableName,
  ]);

  const refreshStatementsList = useCallback(() => {
    const noData = lastStatementsUpdated == null;
    const movingTimeScaleWithPeriodPassed =
      timeScale.key !== "Custom" &&
      moment().diff(lastStatementsUpdated, "minutes") >= 5;
    if ((noData || movingTimeScaleWithPeriodPassed) && details.loaded) {
      const req: StatementsUsingIndexRequest = StatementsListRequestFromDetails(
        details.tableID,
        details.indexID,
        databaseName,
        timeScale,
      );
      getStatementsUsingIndex(req)
        .then(res => {
          populateRegionNodeForStatements(res.results, nodeRegions);
          setStatements(res.results);
          setLastStatementsUpdated(moment());
          setLastStatementsError(null);
        })
        .catch(error => {
          setLastStatementsUpdated(moment());
          setLastStatementsError(error);
        });
    }
  }, [
    lastStatementsUpdated,
    timeScale,
    details.loaded,
    details.tableID,
    details.indexID,
    databaseName,
    nodeRegions,
  ]);

  // Initial refresh on mount.
  useEffect(() => {
    refresh();
  }, [refresh]);

  // Set up interval for refreshing statements list.
  useEffect(() => {
    refreshDataIntervalRef.current = setInterval(() => {
      refreshStatementsList();
    }, 100);

    return () => {
      if (refreshDataIntervalRef.current) {
        clearInterval(refreshDataIntervalRef.current);
      }
    };
  }, [refreshStatementsList]);

  const onChangeSortSetting = (ss: SortSetting): void => {
    setStmtSortSetting(ss);
  };

  const onChangePage = (current: number, pageSize: number): void => {
    setStmtPagination(prev => ({ ...prev, current, pageSize }));
  };

  const resetPagination = useCallback((): void => {
    setStmtPagination({ current: 1, pageSize: 10 });
  }, []);

  const onSubmitSearchField = useCallback(
    (searchValue: string): void => {
      setSearch(searchValue);
      resetPagination();
    },
    [resetPagination],
  );

  const onClearSearchField = useCallback((): void => {
    onSubmitSearchField("");
  }, [onSubmitSearchField]);

  const onSubmitFilters = useCallback(
    (newFilters: Filters): void => {
      setFilters(newFilters);
      setActiveFilters(calculateActiveFilters(newFilters));
      resetPagination();
    },
    [resetPagination],
  );

  const onClearFilters = useCallback((): void => {
    setFilters(defaultFilters);
    setActiveFilters(0);
    resetPagination();
  }, [resetPagination]);

  const changeTimeScale = useCallback(
    (ts: TimeScale): void => {
      if (onTimeScaleChange) {
        onTimeScaleChange(ts);
      }
      setLastStatementsUpdated(null);
    },
    [onTimeScaleChange],
  );

  const getTimestamp = (timestamp: Moment) => {
    const minDate = moment.utc("0001-01-01"); // minimum value as per UTC
    if (timestamp.isSame(minDate)) {
      return <>Never</>;
    } else {
      return <Timestamp time={timestamp} format={DATE_FORMAT_24_TZ} />;
    }
  };

  const getApps = (): string[] => {
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
  };

  const renderIndexRecommendations = (
    indexRecommendations: IndexRecommendation[],
  ) => {
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
  };

  const renderBreadcrumbs = () => {
    return (
      <Breadcrumbs
        items={[
          { link: DB_PAGE_PATH, name: "Databases" },
          {
            link: databaseDetailsPagePath(details.databaseID),
            name: databaseName,
          },
          {
            link: tableDetailsPagePath(parseInt(details.tableID, 10)),
            name: `Table: ${tableName}`,
          },
          {
            link: EncodeDatabaseTableIndexUri(
              databaseName,
              tableName,
              indexName,
            ),
            name: `Index: ${indexName}`,
          },
        ]}
        divider={<Icon iconName="CaretRight" size="tiny" />}
        className={cx("header-breadcrumbs")}
      />
    );
  };

  const filteredStatements = (): AggregateStatistics[] => {
    let filteredStmts = statements;
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

      filteredStmts = statements.filter(
        (statement: AggregateStatistics) =>
          (showInternal && isInternal(statement)) ||
          criteria.includes(statement.applicationName),
      );
    }
    return filterStatementsData(filters, search, filteredStmts, isTenant);
  };

  const apps = getApps();
  const nodes = Object.keys(nodeRegions)
    .map(n => Number(n))
    .sort();
  const regions = unique(
    isTenant
      ? flatMap(statements, statement => statement.stats.regions)
      : nodes.map(node => nodeRegions[node.toString()]),
  ).sort();

  const filteredStmts = filteredStatements();

  return (
    <div className={cx("page-container")}>
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          {renderBreadcrumbs()}
        </section>
        <div className={cx("header-container")}>
          <h3
            className={`${baseHeadingClasses.tableName} ${cx(
              "icon__container",
            )}`}
          >
            <IndexIcon className={cx("icon--md", "icon--title")} />
            {indexName}
          </h3>
          <div className={cx("reset-info")}>
            <Tooltip
              placement="bottom"
              title="Index stats accumulate from the time the index was created or had its stats reset.. Clicking 'Reset all index stats' will reset index stats for the entire cluster. Last reset is the timestamp at which the last reset started."
            >
              <div className={cx("last-reset", "underline")}>
                Last reset: {getTimestamp(details.lastReset)}
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
                  onClick={() => resetIndexUsageStats(databaseName, tableName)}
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
                value={details.createStatement}
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
                          {Count(details.totalReads)}
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
                        <h4 className={cx("summary-card--label")}>Last Read</h4>
                      </td>
                      <td className="table__cell">
                        <p className={cx("summary-card--value")}>
                          {getTimestamp(details.lastRead)}
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
                    {renderIndexRecommendations(details.indexRecommendations)}
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
                      onSubmit={onSubmitSearchField}
                      onClear={onClearSearchField}
                      defaultValue={search}
                    />
                  </PageConfigItem>
                  <PageConfigItem>
                    <Filter
                      onSubmitFilters={onSubmitFilters}
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
                      currentScale={timeScale}
                      setTimeScale={changeTimeScale}
                    />
                  </PageConfigItem>
                </PageConfig>
                <Loading
                  loading={statements == null}
                  page="index details"
                  error={lastStatementsError}
                  renderError={() =>
                    LoadingError({
                      statsType: "statements",
                      error: lastStatementsError,
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
                    onClearFilters={onClearFilters}
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
                    onChangeSortSetting={onChangeSortSetting}
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
                    onChange={onChangePage}
                    onShowSizeChange={onChangePage}
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
