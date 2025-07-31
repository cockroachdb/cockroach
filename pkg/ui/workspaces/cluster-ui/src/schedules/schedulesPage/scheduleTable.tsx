// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Nodes, MagnifyingGlass } from "@cockroachlabs/icons";
import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import isEqual from "lodash/isEqual";
import map from "lodash/map";
import React from "react";
import { Link } from "react-router-dom";

import { Anchor } from "src/anchor";
import { Schedule, Schedules } from "src/api/schedulesApi";
import { EmptyTable } from "src/empty";
import { Pagination, ResultsPerPageLabel } from "src/pagination";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { dropSchedules, pauseSchedules, resumeSchedules } from "src/util/docs";
import { DATE_FORMAT } from "src/util/format";

import { Timestamp, Timezone } from "../../timestamp";
import styles from "../schedules.module.scss";
const cx = classNames.bind(styles);

class SchedulesSortedTable extends SortedTable<Schedule> {}

const schedulesTableColumns: ColumnDescriptor<Schedule>[] = [
  {
    name: "scheduleId",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            {"Unique schedule ID. This value is used to "}
            <Anchor href={pauseSchedules} target="_blank">
              pause
            </Anchor>
            {", "}
            <Anchor href={resumeSchedules} target="_blank">
              resume
            </Anchor>
            {", or "}
            <Anchor href={dropSchedules} target="_blank">
              cancel
            </Anchor>
            {" schedules."}
          </p>
        }
      >
        {"Schedule ID"}
      </Tooltip>
    ),
    titleAlign: "right",
    cell: schedule => {
      return (
        <Link to={`schedules/${String(schedule.id)}`}>
          {String(schedule.id)}
        </Link>
      );
    },
    sort: schedule => schedule.id,
  },
  {
    name: "label",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>The schedule&apos;s label.</p>}
      >
        {"Label"}
      </Tooltip>
    ),
    className: cx("cl-table__col-query-text"),
    cell: schedule => schedule.label,
    sort: schedule => schedule.label,
  },
  {
    name: "status",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Current schedule status.</p>}
      >
        {"Status"}
      </Tooltip>
    ),
    cell: schedule => schedule.status,
    sort: schedule => schedule.status,
  },
  {
    name: "nextRun",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Date and time the schedule will next execute.</p>}
      >
        <>
          Next Execution Time <Timezone />
        </>
      </Tooltip>
    ),
    cell: schedule =>
      schedule.nextRun ? (
        <Timestamp time={schedule.nextRun} format={DATE_FORMAT} />
      ) : (
        <>N/A</>
      ),
    sort: schedule => schedule.nextRun?.valueOf(),
  },
  {
    name: "recurrence",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>How often the schedule executes.</p>}
      >
        {"Recurrence"}
      </Tooltip>
    ),
    cell: schedule => schedule.recurrence,
    sort: schedule => schedule.recurrence,
  },
  {
    name: "jobsRunning",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Number of jobs currently running.</p>}
      >
        {"Jobs Running"}
      </Tooltip>
    ),
    cell: schedule => String(schedule.jobsRunning),
    sort: schedule => schedule.jobsRunning,
  },
  {
    name: "owner",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>User that created the schedule.</p>}
      >
        {"Owner"}
      </Tooltip>
    ),
    cell: schedule => schedule.owner,
    sort: schedule => schedule.owner,
  },
  {
    name: "creationTime",
    title: (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Date and time the schedule was created.</p>}
      >
        <>
          Creation Time <Timezone />
        </>
      </Tooltip>
    ),
    cell: schedule =>
      schedule.created ? (
        <Timestamp time={schedule.created} format={DATE_FORMAT} />
      ) : (
        <>N/A</>
      ),
    sort: schedule => schedule.created?.valueOf(),
  },
];

export interface ScheduleTableProps {
  sort: SortSetting;
  setSort: (value: SortSetting) => void;
  schedules: Schedules;
  pageSize?: number;
  current?: number;
  isUsedFilter: boolean;
}

export interface ScheduleTableState {
  pagination: {
    pageSize: number;
    current: number;
  };
}

export class ScheduleTable extends React.Component<
  ScheduleTableProps,
  ScheduleTableState
> {
  constructor(props: ScheduleTableProps) {
    super(props);

    this.state = {
      pagination: {
        pageSize: props.pageSize || 20,
        current: props.current || 1,
      },
    };
  }

  componentDidUpdate(prevProps: Readonly<ScheduleTableProps>): void {
    this.setCurrentPageToOneIfSchedulesChanged(prevProps);
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
  };

  renderEmptyState = () => {
    const { isUsedFilter, schedules } = this.props;
    const hasData = schedules?.length > 0;

    if (hasData) {
      return null;
    }

    if (isUsedFilter) {
      return (
        <EmptyTable
          title="No schedules match your search"
          icon={<MagnifyingGlass />}
        />
      );
    } else {
      return (
        <EmptyTable
          title="No schedules to show"
          icon={<Nodes />}
          message="The schedules page provides details about backup/restore schedules, sql operation schedules, and others."
        />
      );
    }
  };

  render() {
    const schedules = this.props.schedules;
    const { pagination } = this.state;

    return (
      <React.Fragment>
        <div className={cx("cl-table-statistic schedules-table-summary")}>
          <h4 className={cx("cl-count-title")}>
            <ResultsPerPageLabel
              pagination={{ ...pagination, total: schedules.length }}
              pageName="schedules"
            />
          </h4>
        </div>
        <SchedulesSortedTable
          data={schedules}
          sortSetting={this.props.sort}
          onChangeSortSetting={this.props.setSort}
          className={cx("schedules-table")}
          rowClass={schedule => cx("schedules-table__row--" + schedule.status)}
          columns={schedulesTableColumns}
          renderNoResult={this.renderEmptyState()}
          pagination={pagination}
          tableWrapperClassName={cx("sorted-table")}
        />
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={schedules.length}
          onChange={this.onChangePage}
        />
      </React.Fragment>
    );
  }

  private setCurrentPageToOneIfSchedulesChanged(
    prevProps: Readonly<ScheduleTableProps>,
  ) {
    if (
      !isEqual(
        map(prevProps.schedules, j => {
          return j.id;
        }),
        map(this.props.schedules, j => {
          return j.id;
        }),
      )
    ) {
      this.setState((prevState: Readonly<ScheduleTableState>) => {
        return {
          pagination: {
            ...prevState.pagination,
            current: 1,
          },
        };
      });
    }
  }
}
