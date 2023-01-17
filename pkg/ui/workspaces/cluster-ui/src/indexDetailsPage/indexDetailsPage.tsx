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
import classNames from "classnames/bind";
import { SortSetting } from "src/sortedtable";
import { UIConfigState } from "../store";
import styles from "./indexDetailsPage.module.scss";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import { CaretRight } from "../icon/caretRight";
import { BreadcrumbItem, Breadcrumbs } from "../breadcrumbs";
import { Caution, Search as IndexIcon } from "@cockroachlabs/icons";
import { SqlBox, SqlBoxSize } from "src/sql";
import { Col, Row, Tooltip } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import "antd/lib/tooltip/style";
import { SummaryCard } from "../summaryCard";
import moment, { Moment } from "moment";
import { Heading } from "@cockroachlabs/ui-components";
import { Anchor } from "../anchor";
import { Count, DATE_FORMAT_24_UTC, performanceTuningRecipes } from "../util";

const cx = classNames.bind(styles);

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
  breadcrumbItems: BreadcrumbItem[];
  hasAdminRole?: UIConfigState["hasAdminRole"];
}

interface IndexDetails {
  loading: boolean;
  loaded: boolean;
  createStatement: string;
  totalReads: number;
  lastRead: Moment;
  lastReset: Moment;
  indexRecommendations: IndexRecommendation[];
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
}

export type IndexDetailsPageProps = IndexDetailsPageData &
  IndexDetailPageActions;

interface IndexDetailsPageState {
  sortSetting: SortSetting;
}

export class IndexDetailsPage extends React.Component<
  IndexDetailsPageProps,
  IndexDetailsPageState
> {
  constructor(props: IndexDetailsPageProps) {
    super(props);

    this.state = {
      sortSetting: {
        ascending: true,
      },
    };
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(): void {
    this.refresh();
  }

  private refresh() {
    this.props.refreshUserSQLRoles();
    if (this.props.refreshNodes != null) {
      this.props.refreshNodes();
    }
    if (!this.props.details.loaded && !this.props.details.loading) {
      return this.props.refreshIndexStats(
        this.props.databaseName,
        this.props.tableName,
      );
    }
  }

  private getTimestampString(timestamp: Moment): string {
    const minDate = moment.utc("0001-01-01"); // minimum value as per UTC
    if (timestamp.isSame(minDate)) {
      return "Never";
    } else {
      return timestamp.format(DATE_FORMAT_24_UTC);
    }
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
    if (this.props.breadcrumbItems) {
      return (
        <Breadcrumbs
          items={this.props.breadcrumbItems}
          divider={<CaretRight className={cx("icon--xxs", "icon--primary")} />}
        />
      );
    }
    // If no props are passed, render db-console breadcrumb links by default.
    return (
      <Breadcrumbs
        items={[
          { link: "/databases", name: "Databases" },
          {
            link: `/database/${this.props.databaseName}`,
            name: "Tables",
          },
          {
            link: `/database/${this.props.databaseName}/table/${this.props.tableName}`,
            name: `Table: ${this.props.tableName}`,
          },
          {
            link: `/database/${this.props.databaseName}/table/${this.props.tableName}/index/${this.props.indexName}`,
            name: `Index: ${this.props.indexName}`,
          },
        ]}
        divider={<CaretRight className={cx("icon--xxs", "icon--primary")} />}
      />
    );
  }

  render(): React.ReactElement {
    const { hasAdminRole } = this.props;

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
                title="Index stats accumulate from the time the index was created or had its stats reset.. Clicking ‘Reset all index stats’ will reset index stats for the entire cluster."
              >
                <div className={cx("last-reset", "underline")}>
                  Last reset:{" "}
                  {this.getTimestampString(this.props.details.lastReset)}
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
                  size={SqlBoxSize.custom}
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
                            {this.getTimestampString(
                              this.props.details.lastRead,
                            )}
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
                  <Heading type="h5">Index recommendations</Heading>
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
          </section>
        </div>
      </div>
    );
  }
}
