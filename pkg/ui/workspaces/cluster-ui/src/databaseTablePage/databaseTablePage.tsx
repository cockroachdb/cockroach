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
import { Col, Row, Tabs, Tooltip } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import "antd/lib/tabs/style";
import { RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import classnames from "classnames/bind";
import "antd/lib/tooltip/style";
import { Heading } from "@cockroachlabs/ui-components";

import { Anchor } from "src/anchor";
import { StackIcon } from "src/icon/stackIcon";
import { SqlBox } from "src/sql";
import { ColumnDescriptor, SortedTable, SortSetting } from "src/sortedtable";
import {
  SummaryCard,
  SummaryCardItem,
  SummaryCardItemBoolSetting,
} from "src/summaryCard";
import * as format from "src/util/format";
import { DATE_FORMAT_24_TZ } from "src/util/format";
import {
  ascendingAttr,
  columnTitleAttr,
  syncHistory,
  tabAttr,
  tableStatsClusterSetting,
} from "src/util";

import styles from "./databaseTablePage.module.scss";
import { commonStyles } from "src/common";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import { Moment } from "moment-timezone";
import booleanSettingStyles from "../settings/booleanSetting.module.scss";
import { CockroachCloudContext } from "../contexts";
import { RecommendationType } from "../indexDetailsPage";
import LoadingError from "../sqlActivity/errorComponent";
import { Loading } from "../loading";
import { UIConfigState } from "../store";
import { Timestamp, Timezone } from "../timestamp";
import {
  ActionCell,
  DbTablesBreadcrumbs,
  FormatMVCCInfo,
  IndexRecCell,
  LastReset,
  LastUsed,
  NameCell,
} from "./helperComponents";

const cx = classNames.bind(styles);
const booleanSettingCx = classnames.bind(booleanSettingStyles);

const { TabPane } = Tabs;

// We break out separate interfaces for some of the nested objects in our data
// so that we can make (typed) test assertions on narrower slices of the data.
//
// The loading and loaded flags help us know when to dispatch the appropriate
// refresh actions.
//
// The overall structure is:
//
//   interface DatabaseTablePageData {
//     databaseName: string;
//     name: string;
//     details: { // DatabaseTablePageDataDetails
//       loading: boolean;
//       loaded: boolean;
//       createStatement: string;
//       replicaCount: number;
//       indexNames: string[];
//       grants: {
//         user: string;
//         privilege: string;
//       }[];
//     };
//     stats: { // DatabaseTablePageDataStats
//       loading: boolean;
//       loaded: boolean;
//       sizeInBytes: number;
//       rangeCount: number;
//       nodesByRegionString: string;
//     };
//     indexStats: { // DatabaseTablePageIndexStats
//       loading: boolean;
//       loaded: boolean;
//       stats: {
//         indexName: string;
//         totalReads: number;
//         lastUsed: Moment;
//         lastUsedType: string;
//       }[];
//       lastReset: Moment;
//     };
//   }
export interface DatabaseTablePageData {
  databaseName: string;
  name: string;
  schemaName: string;
  details: DatabaseTablePageDataDetails;
  indexStats: DatabaseTablePageIndexStats;
  showNodeRegionsSection?: boolean;
  indexUsageStatsEnabled: boolean;
  showIndexRecommendations: boolean;
  automaticStatsCollectionEnabled?: boolean;
  hasAdminRole?: UIConfigState["hasAdminRole"];
  csIndexUnusedDuration: string;
}

export interface DatabaseTablePageDataDetails {
  loading: boolean;
  loaded: boolean;
  lastError: Error;
  createStatement: string;
  replicaCount: number;
  indexNames: string[];
  grants: Grant[];
  statsLastUpdated?: Moment;
  totalBytes: number;
  liveBytes: number;
  livePercentage: number;
  sizeInBytes: number;
  rangeCount: number;
  nodesByRegionString?: string;
}

export interface DatabaseTablePageIndexStats {
  loading: boolean;
  loaded: boolean;
  lastError: Error;
  stats: IndexStat[];
  lastReset: Moment;
}

export interface IndexStat {
  indexName: string;
  totalReads: number;
  lastUsed: Moment;
  lastUsedType: string;
  indexRecommendations: IndexRecommendation[];
}

interface IndexRecommendation {
  type: RecommendationType;
  reason: string;
}

interface Grant {
  user: string;
  privileges: string[];
}

export interface DatabaseTablePageActions {
  refreshTableDetails: (
    database: string,
    table: string,
    csIndexUnusedDuration: string,
  ) => void;
  refreshSettings: () => void;
  refreshIndexStats?: (database: string, table: string) => void;
  resetIndexUsageStats?: (database: string, table: string) => void;
  refreshNodes?: () => void;
  refreshUserSQLRoles: () => void;
}

export type DatabaseTablePageProps = DatabaseTablePageData &
  DatabaseTablePageActions &
  RouteComponentProps;

interface DatabaseTablePageState {
  grantSortSetting: SortSetting;
  indexSortSetting: SortSetting;
  tab: string;
  indexStatsColumns: ColumnDescriptor<IndexStat>[];
}

const indexTabKey = "overview";
const grantsTabKey = "grants";

class DatabaseTableGrantsTable extends SortedTable<Grant> {}

class IndexUsageStatsTable extends SortedTable<IndexStat> {}

export class DatabaseTablePage extends React.Component<
  DatabaseTablePageProps,
  DatabaseTablePageState
> {
  static contextType = CockroachCloudContext;

  constructor(props: DatabaseTablePageProps) {
    super(props);

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const currentTab = searchParams.get(tabAttr) || indexTabKey;
    const indexSort: SortSetting = {
      ascending: true,
      columnTitle: "last used",
    };

    const grantSort: SortSetting = {
      ascending: true,
      columnTitle: "username",
    };

    const columnTitle = searchParams.get(columnTitleAttr);
    if (columnTitle) {
      if (currentTab === grantsTabKey) {
        grantSort.columnTitle = columnTitle;
      } else {
        indexSort.columnTitle = columnTitle;
      }
    }

    this.state = {
      indexSortSetting: indexSort,
      grantSortSetting: grantSort,
      tab: currentTab,
      indexStatsColumns: this.indexStatsColumns(),
    };
  }

  onTabChange = (tab: string): void => {
    this.setState({ ...this.state, tab });

    this.updateUrlAttrFromState(
      tab === grantsTabKey
        ? this.state.grantSortSetting
        : this.state.indexSortSetting,
    );

    syncHistory(
      {
        tab: tab,
      },
      this.props.history,
    );
  };

  componentDidMount(): void {
    this.refresh();
    if (this.props.refreshSettings != null) {
      this.props.refreshSettings();
    }
  }

  componentDidUpdate(prevProp: Readonly<DatabaseTablePageProps>): void {
    this.refresh();
    if (
      prevProp.showIndexRecommendations !== this.props.showIndexRecommendations
    ) {
      this.setState({ indexStatsColumns: this.indexStatsColumns() });
    }
  }

  private refresh() {
    this.props.refreshUserSQLRoles();
    if (this.props.refreshNodes != null) {
      this.props.refreshNodes();
    }

    if (
      !this.props.details.loaded &&
      !this.props.details.loading &&
      this.props.details.lastError === undefined
    ) {
      return this.props.refreshTableDetails(
        this.props.databaseName,
        this.props.name,
        this.props.csIndexUnusedDuration,
      );
    }

    if (
      !this.props.indexStats.loaded &&
      !this.props.indexStats.loading &&
      this.props.indexUsageStatsEnabled
    ) {
      return this.props.refreshIndexStats(
        this.props.databaseName,
        this.props.name,
      );
    }
  }

  private changeIndexSortSetting(sortSetting: SortSetting) {
    const stateCopy = { ...this.state };
    stateCopy.indexSortSetting = sortSetting;
    this.setState(stateCopy);
    this.updateUrlAttrFromState(sortSetting);
  }

  private changeGrantSortSetting(sortSetting: SortSetting) {
    const stateCopy = { ...this.state };
    stateCopy.grantSortSetting = sortSetting;
    this.setState(stateCopy);
    this.updateUrlAttrFromState(sortSetting);
  }

  private updateUrlAttrFromState(sortSetting: SortSetting) {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    searchParams.set(columnTitleAttr, sortSetting.columnTitle);
    searchParams.set(ascendingAttr, String(sortSetting.ascending));
    history.location.search = searchParams.toString();
    history.replace(history.location);
  }

  private indexStatsColumns(): ColumnDescriptor<IndexStat>[] {
    const indexStatsColumns: ColumnDescriptor<IndexStat>[] = [
      {
        name: "indexes",
        title: "Indexes",
        hideTitleUnderline: true,
        className: cx("index-stats-table__col-indexes"),
        cell: indexStat => (
          <NameCell
            indexStat={indexStat}
            tableName={this.props.name}
            showIndexRecommendations={this.props.showIndexRecommendations}
          />
        ),
        sort: indexStat => indexStat.indexName,
      },
      {
        name: "total reads",
        title: "Total Reads",
        hideTitleUnderline: true,
        cell: indexStat => format.Count(indexStat.totalReads),
        sort: indexStat => indexStat.totalReads,
      },
      {
        name: "last used",
        title: (
          <>
            Last Used <Timezone />
          </>
        ),
        hideTitleUnderline: true,
        className: cx("index-stats-table__col-last-used"),
        cell: indexStat => <LastUsed indexStat={indexStat} />,
        sort: indexStat => indexStat.lastUsed,
      },
    ];
    if (this.props.showIndexRecommendations) {
      indexStatsColumns.push({
        name: "index recommendations",
        title: (
          <Tooltip
            placement="bottom"
            title="Index recommendations will appear if the system detects improper index usage, such as the occurrence of unused indexes. Following index recommendations may help improve query performance."
          >
            Index Recommendations
          </Tooltip>
        ),
        cell: indexStat => <IndexRecCell indexStat={indexStat} />,
        sort: indexStat => indexStat.indexRecommendations.length,
      });
      const isCockroachCloud = this.context;
      if (!isCockroachCloud) {
        indexStatsColumns.push({
          name: "action",
          title: "",
          cell: indexStat => (
            <ActionCell
              indexStat={indexStat}
              databaseName={this.props.databaseName}
              tableName={this.props.name}
            />
          ),
        });
      }
    }
    return indexStatsColumns;
  }

  private grantsColumns: ColumnDescriptor<Grant>[] = [
    {
      name: "username",
      title: (
        <Tooltip placement="bottom" title="The user name.">
          User Name
        </Tooltip>
      ),
      cell: grant => grant.user,
      sort: grant => grant.user,
    },
    {
      name: "privilege",
      title: (
        <Tooltip placement="bottom" title="The list of grants for the user.">
          Grants
        </Tooltip>
      ),
      cell: grant => grant.privileges.join(", "),
      sort: grant => grant.privileges.join(", "),
    },
  ];

  render(): React.ReactElement {
    const { hasAdminRole } = this.props;
    return (
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          <DbTablesBreadcrumbs
            tableName={this.props.name}
            schemaName={this.props.schemaName}
            databaseName={this.props.databaseName}
          />
          <h3
            className={`${baseHeadingClasses.tableName} ${cx(
              "icon__container",
            )}`}
          >
            <StackIcon className={cx("icon--md", "icon--title")} />
            {this.props.name}
          </h3>
        </section>

        <section className={(baseHeadingClasses.wrapper, cx("tab-area"))}>
          <Tabs
            className={commonStyles("cockroach--tabs")}
            onChange={this.onTabChange}
            activeKey={this.state.tab}
          >
            <TabPane
              tab="Overview"
              key={indexTabKey}
              className={cx("tab-pane")}
            >
              <Loading
                loading={this.props.details.loading}
                page={"table_details"}
                error={this.props.details.lastError}
                render={() => (
                  <>
                    <Row gutter={18}>
                      <Col className="gutter-row" span={18}>
                        <SqlBox value={this.props.details.createStatement} />
                      </Col>
                    </Row>

                    <Row gutter={18}>
                      <Col className="gutter-row" span={8}>
                        <SummaryCard className={cx("summary-card")}>
                          <SummaryCardItem
                            label="Size"
                            value={format.Bytes(this.props.details.sizeInBytes)}
                          />
                          <SummaryCardItem
                            label="Replicas"
                            value={this.props.details.replicaCount}
                          />
                          <SummaryCardItem
                            label="Ranges"
                            value={this.props.details.rangeCount}
                          />
                          <SummaryCardItem
                            label="% of Live Data"
                            value={
                              <FormatMVCCInfo details={this.props.details} />
                            }
                          />
                          {this.props.details.statsLastUpdated && (
                            <SummaryCardItem
                              label="Table Stats Last Updated"
                              value={
                                <Timestamp
                                  time={this.props.details.statsLastUpdated}
                                  format={DATE_FORMAT_24_TZ}
                                />
                              }
                            />
                          )}
                          {this.props.automaticStatsCollectionEnabled !=
                            null && (
                            <SummaryCardItemBoolSetting
                              label="Auto Stats Collection"
                              value={this.props.automaticStatsCollectionEnabled}
                              toolTipText={
                                <span>
                                  {" "}
                                  Automatic statistics can help improve query
                                  performance. Learn how to{" "}
                                  <Anchor
                                    href={tableStatsClusterSetting}
                                    target="_blank"
                                    className={booleanSettingCx(
                                      "crl-hover-text__link-text",
                                    )}
                                  >
                                    manage statistics collection
                                  </Anchor>
                                  .
                                </span>
                              }
                            />
                          )}
                        </SummaryCard>
                      </Col>

                      <Col className="gutter-row" span={10}>
                        <SummaryCard className={cx("summary-card")}>
                          {this.props.showNodeRegionsSection && (
                            <SummaryCardItem
                              label="Regions/Nodes"
                              value={this.props.details.nodesByRegionString}
                            />
                          )}
                          <SummaryCardItem
                            label="Database"
                            value={this.props.databaseName}
                          />
                          <SummaryCardItem
                            label="Indexes"
                            value={this.props.details.indexNames.join(", ")}
                            className={cx(
                              "database-table-page__indexes--value",
                            )}
                          />
                        </SummaryCard>
                      </Col>
                    </Row>
                    {this.props.indexUsageStatsEnabled && (
                      <Row gutter={18} className={cx("row-spaced")}>
                        <SummaryCard
                          className={cx(
                            "summary-card",
                            "index-stats__summary-card",
                          )}
                        >
                          <div className={cx("index-stats__header")}>
                            <Heading type="h5">Index Stats</Heading>
                            <div className={cx("index-stats__reset-info")}>
                              <Tooltip
                                placement="bottom"
                                title="Index stats accumulate from the time the index was created or had its stats reset. Clicking ‘Reset all index stats’ will reset index stats for the entire cluster. Last reset is the timestamp at which the last reset started."
                              >
                                <div
                                  className={cx(
                                    "index-stats__last-reset",
                                    "underline",
                                  )}
                                >
                                  <LastReset
                                    lastReset={this.props.indexStats.lastReset}
                                  />
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
                                        this.props.name,
                                      )
                                    }
                                  >
                                    Reset all index stats
                                  </a>
                                </div>
                              )}
                            </div>
                          </div>
                          <IndexUsageStatsTable
                            className="index-stats-table"
                            data={this.props.indexStats.stats}
                            columns={this.state.indexStatsColumns}
                            sortSetting={this.state.indexSortSetting}
                            onChangeSortSetting={this.changeIndexSortSetting.bind(
                              this,
                            )}
                            loading={this.props.indexStats.loading}
                          />
                        </SummaryCard>
                      </Row>
                    )}
                  </>
                )}
                renderError={() =>
                  LoadingError({
                    statsType: "databases",
                    error: this.props.details.lastError,
                  })
                }
              />
            </TabPane>
            <TabPane tab="Grants" key={grantsTabKey} className={cx("tab-pane")}>
              <Loading
                loading={this.props.details.loading}
                page={"table_details_grants"}
                error={this.props.details.lastError}
                render={() => (
                  <DatabaseTableGrantsTable
                    data={this.props.details.grants}
                    columns={this.grantsColumns}
                    sortSetting={this.state.grantSortSetting}
                    onChangeSortSetting={this.changeGrantSortSetting.bind(this)}
                    loading={this.props.details.loading}
                    tableWrapperClassName={cx("sorted-table")}
                  />
                )}
                renderError={() =>
                  LoadingError({
                    statsType: "databases",
                    error: this.props.details.lastError,
                  })
                }
              />
            </TabPane>
          </Tabs>
        </section>
      </div>
    );
  }
}
