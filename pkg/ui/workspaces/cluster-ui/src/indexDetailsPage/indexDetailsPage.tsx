// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Caution, Search as IndexIcon } from "@cockroachlabs/icons";
import { Heading, Icon } from "@cockroachlabs/ui-components";
import { Col, Row, Tooltip } from "antd";
import classNames from "classnames/bind";
import flatMap from "lodash/flatMap";
import { Moment } from "moment-timezone";
import React, { useState, useCallback, useContext, useMemo } from "react";

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
  resetIndexStatsApi,
  useTableIndexStats,
  IndexRecommendation,
  IndexRecTypeEnum,
} from "../api/databases/tableIndexesApi";
import {
  getStatementsUsingIndex,
  StatementsListRequestFromDetails,
} from "../api/indexDetailsApi";
import { useNodes } from "../api/nodesApi";
import { useUserSQLRoles } from "../api/userApi";
import { commonStyles } from "../common";
import { ClusterDetailsContext } from "../contexts";
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
import { SummaryCard } from "../summaryCard";
import { TableStatistics } from "../tableStatistics";
import {
  TimeScale,
  timeScale1hMinOptions,
  TimeScaleDropdown,
  toRoundedDateRange,
} from "../timeScaleDropdown";
import {
  calculateTotalWorkload,
  Count,
  DATE_FORMAT_24_TZ,
  EncodeDatabaseTableIndexUri,
  performanceTuningRecipes,
  unique,
  unset,
  useSwrWithClusterId,
} from "../util";
import {
  databaseDetailsPagePath,
  DB_PAGE_PATH,
  tableDetailsPagePath,
} from "../util/routes";

import styles from "./indexDetailsPage.module.scss";

const cx = classNames.bind(styles);
const stmtCx = classNames.bind(statementsStyles);

// Parses a SQL-quoted schema-qualified table name into its parts.
// e.g. '"public"."mytable"' -> { schema: "public", table: "mytable" }
export function parseSchemaQualifiedTableName(qualifiedName: string): {
  schema: string;
  table: string;
} {
  const splitIdx = qualifiedName.indexOf('"."');
  if (splitIdx >= 0) {
    return {
      schema: qualifiedName.substring(1, splitIdx),
      table: qualifiedName.substring(splitIdx + 3, qualifiedName.length - 1),
    };
  }
  const dotIdx = qualifiedName.indexOf(".");
  if (dotIdx >= 0) {
    return {
      schema: qualifiedName.substring(0, dotIdx),
      table: qualifiedName.substring(dotIdx + 1),
    };
  }
  return { schema: "public", table: qualifiedName };
}

export interface IndexDetailsPageProps {
  databaseName: string;
  tableName: string;
  indexName: string;
  timeScale: TimeScale;
  onTimeScaleChange: (ts: TimeScale) => void;
}

export function IndexDetailsPage(
  props: IndexDetailsPageProps,
): React.ReactElement {
  const { databaseName, tableName, indexName, timeScale, onTimeScaleChange } =
    props;

  const { isTenant } = useContext(ClusterDetailsContext);

  // Parse the schema-qualified table name for the index stats hook.
  const { schema: schemaName, table: unqualifiedTableName } = useMemo(
    () => parseSchemaQualifiedTableName(tableName),
    [tableName],
  );

  // Fetch index stats for the table.
  const { indexStats, refreshIndexStats } = useTableIndexStats({
    dbName: databaseName,
    schemaName,
    tableName: unqualifiedTableName,
  });

  // Find the specific index by name.
  const indexDetail = useMemo(
    () => indexStats.tableIndexes.find(idx => idx.indexName === indexName),
    [indexStats.tableIndexes, indexName],
  );

  // Fetch node statuses for region info.
  const { nodeRegionsByID: nodeRegions } = useNodes();

  // Fetch user SQL roles.
  const { data: userSQLRolesResp } = useUserSQLRoles();
  const hasAdminRole = userSQLRolesResp?.roles?.includes("ADMIN");
  const hasViewActivityRedactedRole = userSQLRolesResp?.roles?.includes(
    "VIEWACTIVITYREDACTED",
  );

  const [stmtSortSetting, setStmtSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "time",
  });
  const [stmtPagination, setStmtPagination] = useState<ISortedTablePagination>({
    pageSize: 10,
    current: 1,
  });
  const [search, setSearch] = useState<string>(null);
  const [activeFilters, setActiveFilters] = useState<number>(0);
  const [filters, setFilters] = useState<Filters>(defaultFilters);

  // Build the SWR key for statements using this index.
  // Include timeScale range so key changes when the window changes,
  // triggering a refetch. Pass null when index details aren't loaded
  // yet to disable fetching.
  const statementsSwrKey = useMemo(() => {
    if (!indexDetail) return null;
    const dateRange: [Moment | null, Moment | null] = timeScale
      ? toRoundedDateRange(timeScale)
      : [null, null];
    return {
      name: "statementsUsingIndex",
      tableID: indexDetail.tableID,
      indexID: indexDetail.indexID,
      databaseName,
      start: dateRange[0]?.toISOString(),
      end: dateRange[1]?.toISOString(),
    };
  }, [indexDetail, databaseName, timeScale]);

  const { data: statementsData, error: lastStatementsError } =
    useSwrWithClusterId(
      statementsSwrKey,
      statementsSwrKey
        ? () =>
            getStatementsUsingIndex(
              StatementsListRequestFromDetails(
                indexDetail.tableID,
                indexDetail.indexID,
                databaseName,
                timeScale,
              ),
            )
        : null,
      {
        refreshInterval: 5 * 60 * 1000, // 5 minutes
        dedupingInterval: 5 * 60 * 1000,
        revalidateOnFocus: false,
      },
    );

  // Post-process statement results with node region info.
  const statements = useMemo(() => {
    if (!statementsData?.results) return [];
    const results = [...statementsData.results];
    populateRegionNodeForStatements(results, nodeRegions);
    return results;
  }, [statementsData, nodeRegions]);

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

  const handleResetIndexStats = useCallback(async () => {
    try {
      await resetIndexStatsApi();
      refreshIndexStats();
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error("Failed to reset index stats:", e);
    }
  }, [refreshIndexStats]);

  const getTimestampOrNull = (timestamp: Moment | null) => {
    if (timestamp == null) {
      return <>Never</>;
    }
    return <Timestamp time={timestamp} format={DATE_FORMAT_24_TZ} />;
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
        case IndexRecTypeEnum.DROP_UNUSED:
          recommendationType = "Drop unused index";
          break;
        default:
          recommendationType = "Unknown";
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
    const databaseID = indexStats.databaseID;
    const tableID = indexDetail?.tableID;
    return (
      <Breadcrumbs
        items={[
          { link: DB_PAGE_PATH, name: "Databases" },
          {
            link: databaseDetailsPagePath(databaseID),
            name: databaseName,
          },
          {
            link: tableDetailsPagePath(tableID ? parseInt(tableID, 10) : 0),
            name: `Table: ${unqualifiedTableName}`,
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

  const totalReads = indexDetail?.totalReads ?? 0;
  const lastRead = indexDetail?.lastRead;
  const lastReset = indexStats.lastReset;
  const createStatement = indexDetail?.createStatement ?? "";
  const indexRecommendations = indexDetail?.indexRecs ?? [];

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
                Last reset: {getTimestampOrNull(lastReset)}
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
                  onClick={handleResetIndexStats}
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
              <SqlBox value={createStatement} size={SqlBoxSize.CUSTOM} />
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
                          {Count(totalReads)}
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
                          {getTimestampOrNull(lastRead)}
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
                    {renderIndexRecommendations(indexRecommendations)}
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
                      setTimeScale={onTimeScaleChange}
                    />
                  </PageConfigItem>
                </PageConfig>
                <Loading
                  loading={!statementsData && !lastStatementsError}
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
