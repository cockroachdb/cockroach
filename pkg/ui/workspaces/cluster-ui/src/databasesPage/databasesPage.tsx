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
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import _ from "lodash";

import { StackIcon } from "src/icon/stackIcon";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortSetting,
  SortedTable,
} from "src/sortedtable";
import * as format from "src/util/format";

import styles from "./databasesPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import {
  baseHeadingClasses,
  statisticsClasses,
} from "src/transactionsPage/transactionsPageClasses";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

// We break out separate interfaces for some of the nested objects in our data
// both so that they can be available as SortedTable rows and for making
// (typed) test assertions on narrower slices of the data.
//
// The loading and loaded flags help us know when to dispatch the appropriate
// refresh actions.
//
// The overall structure is:
//
//   interface DatabasesPageData {
//     loading: boolean;
//     loaded: boolean;
//     databases: { // DatabasesPageDataDatabase[]
//       loading: boolean;
//       loaded: boolean;
//       name: string;
//       sizeInBytes: number;
//       tableCount: number;
//       rangeCount: number;
//       missingTables: { // DatabasesPageDataMissingTable[]
//         loading: boolean;
//         name: string;
//       }[];
//     }[];
//   }
export interface DatabasesPageData {
  loading: boolean;
  loaded: boolean;
  databases: DatabasesPageDataDatabase[];
}

export interface DatabasesPageDataDatabase {
  loading: boolean;
  loaded: boolean;
  name: string;
  sizeInBytes: number;
  tableCount: number;
  rangeCount: number;
  missingTables: DatabasesPageDataMissingTable[];
}

// A "missing" table is one for which we were unable to gather size and range
// count information during the backend call to fetch DatabaseDetails. We
// expose it here so that the component has the opportunity to try to refresh
// those properties for the table directly.
export interface DatabasesPageDataMissingTable {
  // Note that we don't need a "loaded" property here because we expect
  // the reducers supplying our properties to remove a missing table from
  // the list once we've loaded its data.
  loading: boolean;
  name: string;
}

export interface DatabasesPageActions {
  refreshDatabases: () => void;
  refreshDatabaseDetails: (database: string) => void;
  refreshTableStats: (database: string, table: string) => void;
}

export type DatabasesPageProps = DatabasesPageData & DatabasesPageActions;

interface DatabasesPageState {
  pagination: ISortedTablePagination;
  sortSetting: SortSetting;
}

class DatabasesSortedTable extends SortedTable<DatabasesPageDataDatabase> {}

export class DatabasesPage extends React.Component<
  DatabasesPageProps,
  DatabasesPageState
> {
  constructor(props: DatabasesPageProps) {
    super(props);

    this.state = {
      pagination: {
        current: 1,
        pageSize: 20,
      },
      sortSetting: {
        ascending: true,
        columnTitle: null,
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
    if (!this.props.loaded && !this.props.loading) {
      return this.props.refreshDatabases();
    }

    _.forEach(this.props.databases, database => {
      if (!database.loaded && !database.loading) {
        return this.props.refreshDatabaseDetails(database.name);
      }

      _.forEach(database.missingTables, table => {
        if (!table.loading) {
          return this.props.refreshTableStats(database.name, table.name);
        }
      });
    });
  }

  private changePage(current: number) {
    this.setState({ pagination: { ...this.state.pagination, current } });
  }

  private changeSortSetting(sortSetting: SortSetting) {
    this.setState({ sortSetting });
  }

  private columns: ColumnDescriptor<DatabasesPageDataDatabase>[] = [
    {
      title: (
        <Tooltip placement="bottom" title="The name of the database.">
          Databases
        </Tooltip>
      ),
      cell: database => (
        <Link
          to={`/database/${database.name}`}
          className={cx("icon__container")}
        >
          <StackIcon className={cx("icon--s", "icon--primary")} />
          {database.name}
        </Link>
      ),
      sort: database => database.name,
      className: cx("databases-table__col-name"),
      name: "name",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="The approximate total disk size across all table replicas in the database."
        >
          Size
        </Tooltip>
      ),
      cell: database => format.Bytes(database.sizeInBytes),
      sort: database => database.sizeInBytes,
      className: cx("databases-table__col-size"),
      name: "size",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="The total number of tables in the database."
        >
          Tables
        </Tooltip>
      ),
      cell: database => database.tableCount,
      sort: database => database.tableCount,
      className: cx("databases-table__col-table-count"),
      name: "tableCount",
    },
    {
      title: (
        <Tooltip
          placement="bottom"
          title="The total number of ranges across all tables in the database."
        >
          Range count
        </Tooltip>
      ),
      cell: database => database.rangeCount,
      sort: database => database.rangeCount,
      className: cx("databases-table__col-range-count"),
      name: "rangeCount",
    },
  ];

  render() {
    return (
      <div>
        <section className={baseHeadingClasses.wrapper}>
          <h1 className={baseHeadingClasses.tableName}>Databases</h1>
        </section>

        <section className={sortableTableCx("cl-table-container")}>
          <div className={statisticsClasses.statistic}>
            <h4 className={statisticsClasses.countTitle}>
              <ResultsPerPageLabel
                pagination={{
                  ...this.state.pagination,
                  total: this.props.databases.length,
                }}
                pageName={
                  this.props.databases.length == 1 ? "database" : "databases"
                }
              />
            </h4>
          </div>

          <DatabasesSortedTable
            className={cx("databases-table")}
            data={this.props.databases}
            columns={this.columns}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting.bind(this)}
            pagination={this.state.pagination}
            loading={this.props.loading}
            renderNoResult={
              <div
                className={cx("databases-table__no-result", "icon__container")}
              >
                <StackIcon className={cx("icon--s")} />
                This cluster has no databases.
              </div>
            }
          />
        </section>

        <Pagination
          pageSize={this.state.pagination.pageSize}
          current={this.state.pagination.current}
          total={this.props.databases.length}
          onChange={this.changePage.bind(this)}
        />
      </div>
    );
  }
}
