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
import { Col, Row, Tabs } from "antd";
import classNames from "classnames/bind";
import _ from "lodash";

import { Breadcrumbs } from "src/breadcrumbs";
import { CaretRight } from "src/icon/caretRight";
import { StackIcon } from "src/icon/stackIcon";
import { SqlBox } from "src/sql";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import * as format from "src/util/format";

import styles from "./databaseTablePage.module.scss";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
const cx = classNames.bind(styles);

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
//     };
//   }
export interface DatabaseTablePageData {
  databaseName: string;
  name: string;
  details: DatabaseTablePageDataDetails;
  stats: DatabaseTablePageDataStats;
}

export interface DatabaseTablePageDataDetails {
  loading: boolean;
  loaded: boolean;
  createStatement: string;
  replicaCount: number;
  indexNames: string[];
  grants: Grant[];
}

interface Grant {
  user: string;
  privilege: string;
}

export interface DatabaseTablePageDataStats {
  loading: boolean;
  loaded: boolean;
  sizeInBytes: number;
  rangeCount: number;
}

export interface DatabaseTablePageActions {
  refreshTableDetails: (database: string, table: string) => void;
  refreshTableStats: (databse: string, table: string) => void;
}

export type DatabaseTablePageProps = DatabaseTablePageData &
  DatabaseTablePageActions;

interface DatabaseTablePageState {
  sortSetting: SortSetting;
}

class DatabaseTableGrantsTable extends SortedTable<Grant> {}

export class DatabaseTablePage extends React.Component<
  DatabaseTablePageProps,
  DatabaseTablePageState
> {
  constructor(props: DatabaseTablePageProps) {
    super(props);

    this.state = {
      sortSetting: {
        ascending: true,
      },
    };
  }

  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate() {
    this.refresh();
  }

  private refresh() {
    if (!this.props.details.loaded && !this.props.details.loading) {
      return this.props.refreshTableDetails(
        this.props.databaseName,
        this.props.name,
      );
    }

    if (!this.props.stats.loaded && !this.props.stats.loading) {
      return this.props.refreshTableStats(
        this.props.databaseName,
        this.props.name,
      );
    }
  }

  private changeSortSetting(sortSetting: SortSetting) {
    this.setState({ sortSetting });
  }

  private columns: ColumnDescriptor<Grant>[] = [
    {
      name: "user",
      title: "User",
      cell: grant => grant.user,
      sort: grant => grant.user,
    },
    {
      name: "privilege",
      title: "Grants",
      cell: grant => grant.privilege,
      sort: grant => grant.privilege,
    },
  ];

  render() {
    return (
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          <Breadcrumbs
            items={[
              { link: "/databases", name: "Databases" },
              { link: `/database/${this.props.databaseName}`, name: "Tables" },
              {
                link: `/database/${this.props.databaseName}/table/${this.props.name}`,
                name: "Table Detail",
              },
            ]}
            divider={
              <CaretRight className={cx("icon--xxs", "icon--primary")} />
            }
          />

          <h1
            className={`${baseHeadingClasses.tableName} ${cx(
              "icon__container",
            )}`}
          >
            <StackIcon className={cx("icon--md", "icon--title")} />
            {this.props.name}
          </h1>
        </section>

        <section className={baseHeadingClasses.wrapper}>
          <Tabs className={cx("cockroach--tabs")}>
            <TabPane tab="Overview" key="overview">
              <Row>
                <Col>
                  <SqlBox value={this.props.details.createStatement} />
                </Col>
              </Row>

              <Row gutter={16}>
                <Col span={10}>
                  <SummaryCard className={cx("summary-card")}>
                    <SummaryCardItem
                      label="Size"
                      value={format.Bytes(this.props.stats.sizeInBytes)}
                    />
                    <SummaryCardItem
                      label="Replicas"
                      value={this.props.details.replicaCount}
                    />
                    <SummaryCardItem
                      label="Ranges"
                      value={this.props.stats.rangeCount}
                    />
                  </SummaryCard>
                </Col>

                <Col span={14}>
                  <SummaryCard className={cx("summary-card")}>
                    <SummaryCardItem
                      label="Database"
                      value={this.props.databaseName}
                    />
                    <SummaryCardItem
                      label="Indexes"
                      value={_.join(this.props.details.indexNames, ", ")}
                      className={cx("database-table-page__indexes--value")}
                    />
                  </SummaryCard>
                </Col>
              </Row>
            </TabPane>

            <TabPane tab="Grants" key="grants">
              <DatabaseTableGrantsTable
                data={this.props.details.grants}
                columns={this.columns}
                sortSetting={this.state.sortSetting}
                onChangeSortSetting={this.changeSortSetting.bind(this)}
                loading={this.props.details.loading}
              />
            </TabPane>
          </Tabs>
        </section>
      </div>
    );
  }
}
