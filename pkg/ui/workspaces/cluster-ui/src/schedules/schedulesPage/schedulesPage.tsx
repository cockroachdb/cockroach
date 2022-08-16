// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { InlineAlert } from "@cockroachlabs/ui-components";
import moment from "moment";
import React from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { SchedulesRequest, SchedulesResponse } from "src/api/schedulesApi";
import { Delayed } from "src/delayed";
import { Dropdown, DropdownOption } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SortSetting } from "src/sortedtable";
import { syncHistory } from "src/util";

import { ScheduleTable } from "./scheduleTable";

import { commonStyles } from "src/common";
import styles from "../schedules.module.scss";
import classNames from "classnames/bind";

import { statusOptions, showOptions } from "./scheduleOptions";

const cx = classNames.bind(styles);

type ScheduleType = cockroach.sql.jobs.jobspb.Type;

export interface SchedulesPageStateProps {
  sort: SortSetting;
  status: string;
  show: string;
  schedules: SchedulesResponse;
  schedulesError: Error | null;
  schedulesLoading: boolean;
}

export interface SchedulesPageDispatchProps {
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  refreshSchedules: (req: SchedulesRequest) => void;
  onFilterChange?: (req: SchedulesRequest) => void;
}

export type SchedulesPageProps = SchedulesPageStateProps &
  SchedulesPageDispatchProps &
  RouteComponentProps;

export class SchedulesPage extends React.Component<SchedulesPageProps> {
  constructor(props: SchedulesPageProps) {
    super(props);

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);

    // Sort Settings.
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting = this.props.sort;
    if (
      this.props.setSort &&
      columnTitle &&
      (sortSetting.columnTitle !== columnTitle ||
        sortSetting.ascending !== ascending)
    ) {
      this.props.setSort({ columnTitle, ascending });
    }

    // Filter Status.
    const status = searchParams.get("status") || undefined;
    if (this.props.setStatus && status && status != this.props.status) {
      this.props.setStatus(status);
    }

    // Filter Show.
    const show = searchParams.get("show") || undefined;
    if (this.props.setShow && show && show !== this.props.show) {
      this.props.setShow(show);
    }
  }

  private refresh(props = this.props): void {
    const schedulesRequest = new cockroach.server.serverpb.SchedulesRequest({
      status: props.status,
      limit: parseInt(props.show, 10),
    });
    props.onFilterChange
      ? props.onFilterChange(schedulesRequest)
      : props.refreshSchedules(schedulesRequest);
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(prevProps: SchedulesPageProps): void {
    if (
      prevProps.status !== this.props.status ||
      prevProps.show !== this.props.show
    ) {
      this.refresh();
    }
  }

  onStatusSelected = (item: string) => {
    this.props.setStatus(item);

    syncHistory(
      {
        status: item,
      },
      this.props.history,
    );
  };

  statusMenuItems: DropdownOption[] = statusOptions;

  onShowSelected = (item: string) => {
    this.props.setShow(item);

    syncHistory(
      {
        show: item,
      },
      this.props.history,
    );
  };

  showMenuItems: DropdownOption[] = showOptions;

  changeSortSetting = (ss: SortSetting): void => {
    if (this.props.setSort) {
      this.props.setSort(ss);
    }

    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
  };

  render() {
    const isLoading = !this.props.schedules || this.props.schedulesLoading;
    const error = this.props.schedules && this.props.schedulesError;
    return (
      <div className={cx("schedules-page")}>
        <Helmet title="Schedules" />
        <h3 className={commonStyles("base-heading")}>Schedules</h3>
        <div>
          <PageConfig>
            <PageConfigItem>
              <Dropdown
                items={this.statusMenuItems}
                onChange={this.onStatusSelected}
              >
                Status:{" "}
                {
                  statusOptions.find(
                    option => option["value"] === this.props.status,
                  )["name"]
                }
              </Dropdown>
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                items={this.showMenuItems}
                onChange={this.onShowSelected}
              >
                Show:{" "}
                {
                  showOptions.find(
                    option => option["value"] === this.props.show,
                  )["name"]
                }
              </Dropdown>
            </PageConfigItem>
          </PageConfig>
        </div>
        <section className={cx("section")}>
          <Loading
            loading={isLoading}
            page={"schedules"}
            error={error}
            render={() => (
              <ScheduleTable
                isUsedFilter={this.props.status.length > 0}
                schedules={this.props.schedules}
                setSort={this.changeSortSetting}
                sort={this.props.sort}
              />
            )}
          />
          {isLoading && !error && (
            <Delayed delay={moment.duration(2, "s")}>
              <InlineAlert
                intent="info"
                title="If the Schedules table contains a large amount of data, this page might take a while to load. To reduce the amount of data, try filtering the table."
              />
            </Delayed>
          )}
        </section>
      </div>
    );
  }
}
