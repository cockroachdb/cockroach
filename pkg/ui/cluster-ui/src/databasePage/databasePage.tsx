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

import { AlignedIcon } from "src/icon/alignedIcon";
import { Breadcrumbs } from "src/breadcrumbs";
import { Dropdown, DropdownOption } from "src/dropdown";
import { EmptyTable } from "src/empty";
import { Icon } from "src/icon";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import {
  ColumnDescriptor,
  ISortedTablePagination,
  SortSetting,
  SortedTable,
} from "src/sortedtable";
import * as format from "src/util/format";

import styles from "./databasePage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import {
  baseHeadingClasses,
  statisticsClasses,
} from "src/transactionsPage/transactionsPageClasses";

const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

export interface DatabasePageDataTableDetails {
  loading: boolean;
  loaded: boolean;
  columnCount: number;
  indexCount: number;
  userCount: number;
  roles: string[];
  grants: string[];
}

export interface DatabasePageDataTableStats {
  loading: boolean;
  loaded: boolean;
  replicationSizeInBytes: number;
  rangeCount: number;
}

export interface DatabasePageDataTable {
  name: string;
  details: DatabasePageDataTableDetails;
  stats: DatabasePageDataTableStats;
}

export interface DatabasePageData {
  loading: boolean;
  loaded: boolean;
  name: string;
  tables: DatabasePageDataTable[];
}

export interface DatabasePageActions {
  refreshDatabaseDetails: (database: string) => void;
  refreshTableDetails: (database: string, table: string) => void;
  refreshTableStats: (database: string, table: string) => void;
}

export type DatabasePageProps = DatabasePageData & DatabasePageActions;

type ViewOption = "Tables" | "Grants";

interface DatabasePageState {
  pagination: ISortedTablePagination;
  sortSetting: SortSetting;
  view: ViewOption;
}

class DatabaseSortedTable extends SortedTable<DatabasePageDataTable> {}

export class DatabasePage extends React.Component<
  DatabasePageProps,
  DatabasePageState
> {
  constructor(props: DatabasePageProps) {
    super(props);

    this.state = {
      pagination: {
        current: 1,
        pageSize: 20,
      },
      sortSetting: {
        ascending: true,
      },
      view: "Tables",
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
      return this.props.refreshDatabaseDetails(this.props.name);
    }

    _.forEach(this.props.tables, table => {
      if (!table.details.loaded && !table.details.loading) {
        return this.props.refreshTableDetails(this.props.name, table.name);
      }

      if (!table.stats.loaded && !table.stats.loading) {
        return this.props.refreshTableStats(this.props.name, table.name);
      }
    });
  }

  private changePage(current: number) {
    this.setState({ pagination: { ...this.state.pagination, current } });
  }

  private changeSortSetting(sortSetting: SortSetting) {
    this.setState({ sortSetting });
  }

  private changeView(view: ViewOption) {
    this.setState({ view });
  }

  private columns(): ColumnDescriptor<DatabasePageDataTable>[] {
    const databaseName = this.props.name;

    const alwaysColumns: ColumnDescriptor<DatabasePageDataTable>[] = [
      {
        title: (
          <Tooltip placement="bottom" title="The name of the table.">
            Tables
          </Tooltip>
        ),
        cell: table => (
          <Link to={`/database/${databaseName}/table/${table.name}`}>
            <AlignedIcon type="table" size="s">
              {table.name}
            </AlignedIcon>
          </Link>
        ),
        sort: table => table.name,
        className: cx("database-table__col-name"),
        name: "name",
      },
    ];

    const tablesViewColumns: ColumnDescriptor<DatabasePageDataTable>[] = [
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The approximate total disk size across all replicas of the table."
          >
            Replication Size
          </Tooltip>
        ),
        cell: table => format.Bytes(table.stats.replicationSizeInBytes),
        sort: table => table.stats.replicationSizeInBytes,
        className: cx("database-table__col-size"),
        name: "replicationSize",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The total number of ranges in the table."
          >
            Ranges
          </Tooltip>
        ),
        cell: table => table.stats.rangeCount,
        sort: table => table.stats.rangeCount,
        className: cx("database-table__col-range-count"),
        name: "rangeCount",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The number of columns in the table."
          >
            Columns
          </Tooltip>
        ),
        cell: table => table.details.columnCount,
        sort: table => table.details.columnCount,
        className: cx("database-table__col-column-count"),
        name: "columnCount",
      },
      {
        title: (
          <Tooltip
            placement="bottom"
            title="The number of indexes in the table."
          >
            Indexes
          </Tooltip>
        ),
        cell: table => table.details.indexCount,
        sort: table => table.details.indexCount,
        className: cx("database-table__col-index-count"),
        name: "indexCount",
      },
    ];

    const grantsViewColumns: ColumnDescriptor<DatabasePageDataTable>[] = [
      {
        title: "Users",
        cell: table => table.details.userCount,
        sort: table => table.details.userCount,
        className: cx("database-table__col-user-count"),
        name: "userCount",
      },
      {
        title: "Roles",
        cell: table => _.join(table.details.roles, ", "),
        sort: table => _.join(table.details.roles, ", "),
        className: cx("database-table__col-roles"),
        name: "roles",
      },
      {
        title: "Grants",
        cell: table => _.join(table.details.grants, ", "),
        sort: table => _.join(table.details.grants, ", "),
        className: cx("database-table__col-grants"),
        name: "grants",
      },
    ];

    switch (this.state.view) {
      case "Tables":
        return _.concat(alwaysColumns, tablesViewColumns);
      case "Grants":
        return _.concat(alwaysColumns, grantsViewColumns);
      default:
        throw new Error(`Unknown view ${this.state.view}`);
    }
  }

  private viewOptions(): DropdownOption<ViewOption>[] {
    return [
      {
        value: "Tables",
        name: "Tables",
      },
      {
        value: "Grants",
        name: "Grants",
      },
    ];
  }

  private viewSelectionTitle(): string {
    return `View: ${this.state.view}`;
  }

  render() {
    return (
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          <Breadcrumbs
            items={[
              { link: "/databases", name: "Databases" },
              { link: `/database/${this.props.name}`, name: "Tables" },
            ]}
            divider={<Icon type="caret-right" size="xxs" />}
          />

          <h1 className={baseHeadingClasses.tableName}>
            <AlignedIcon type="stack" size="md">
              {this.props.name}
            </AlignedIcon>
          </h1>
        </section>

        <PageConfig>
          <PageConfigItem>
            <Dropdown
              items={this.viewOptions()}
              onChange={this.changeView.bind(this)}
            >
              {this.viewSelectionTitle()}
            </Dropdown>
          </PageConfigItem>
        </PageConfig>

        <section className={sortableTableCx("cl-table-container")}>
          <div className={statisticsClasses.statistic}>
            <h4 className={statisticsClasses.countTitle}>
              <ResultsPerPageLabel
                pagination={{
                  ...this.state.pagination,
                  total: this.props.tables.length,
                }}
                pageName={this.props.tables.length == 1 ? "table" : "tables"}
              />
            </h4>
          </div>

          <DatabaseSortedTable
            className={cx("database-table")}
            data={this.props.tables}
            columns={this.columns()}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting.bind(this)}
            pagination={this.state.pagination}
            loading={this.props.loading}
            renderNoResult={
              <EmptyTable
                title={
                  <AlignedIcon type="table" size="md">
                    This database has no tables.
                  </AlignedIcon>
                }
              />
            }
          />
        </section>

        <Pagination
          pageSize={this.state.pagination.pageSize}
          current={this.state.pagination.current}
          total={this.props.tables.length}
          onChange={this.changePage.bind(this)}
        />
      </div>
    );
  }
}
