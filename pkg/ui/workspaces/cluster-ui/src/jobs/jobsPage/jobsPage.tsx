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
import { JobsRequest, JobsResponse } from "src/api/jobsApi";
import { Delayed } from "src/delayed";
import { Dropdown, DropdownOption } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SortSetting } from "src/sortedtable";
import { syncHistory } from "src/util";

import { JobTable } from "./jobTable";
import { statusOptions, showOptions, typeOptions } from "../util";

import { commonStyles } from "src/common";
import styles from "../jobs.module.scss";
import classNames from "classnames/bind";

const cx = classNames.bind(styles);

type JobType = cockroach.sql.jobs.jobspb.Type;

export interface JobsPageStateProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  jobs: JobsResponse;
  jobsError: Error | null;
  jobsLoading: boolean;
}

export interface JobsPageDispatchProps {
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  refreshJobs: (req: JobsRequest) => void;
  onFilterChange?: (req: JobsRequest) => void;
}

export type JobsPageProps = JobsPageStateProps &
  JobsPageDispatchProps &
  RouteComponentProps;

export class JobsPage extends React.Component<JobsPageProps> {
  constructor(props: JobsPageProps) {
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

    // Filter Type.
    const type = parseInt(searchParams.get("type"), 10) || undefined;
    if (this.props.setType && type && type !== this.props.type) {
      this.props.setType(type);
    }
  }

  private refresh(props = this.props): void {
    const jobsRequest = new cockroach.server.serverpb.JobsRequest({
      status: props.status,
      type: props.type,
      limit: parseInt(props.show, 10),
    });
    props.onFilterChange
      ? props.onFilterChange(jobsRequest)
      : props.refreshJobs(jobsRequest);
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(prevProps: JobsPageProps): void {
    if (
      prevProps.status !== this.props.status ||
      prevProps.type !== this.props.type ||
      prevProps.show !== this.props.show
    ) {
      this.refresh();
    }
  }

  onStatusSelected = (item: string): void => {
    this.props.setStatus(item);

    syncHistory(
      {
        status: item,
      },
      this.props.history,
    );
  };

  statusMenuItems: DropdownOption[] = statusOptions;

  onTypeSelected = (item: string): void => {
    const type = parseInt(item, 10);
    this.props.setType(type);

    syncHistory(
      {
        type: type.toString(),
      },
      this.props.history,
    );
  };

  typeMenuItems: DropdownOption[] = typeOptions;

  onShowSelected = (item: string): void => {
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

  render(): React.ReactElement {
    const isLoading = !this.props.jobs || this.props.jobsLoading;
    const error = this.props.jobs && this.props.jobsError;
    return (
      <div className={cx("jobs-page")}>
        <Helmet title="Jobs" />
        <h3 className={commonStyles("base-heading")}>Jobs</h3>
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
                items={this.typeMenuItems}
                onChange={this.onTypeSelected}
              >
                Type:{" "}
                {
                  typeOptions.find(
                    option => option["value"] === this.props.type.toString(),
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
            page={"jobs"}
            error={error}
            render={() => (
              <JobTable
                isUsedFilter={
                  this.props.status.length > 0 || this.props.type > 0
                }
                jobs={this.props.jobs}
                setSort={this.changeSortSetting}
                sort={this.props.sort}
              />
            )}
          />
          {isLoading && !error && (
            <Delayed delay={moment.duration(2, "s")}>
              <InlineAlert
                intent="info"
                title="If the Jobs table contains a large amount of data, this page might take a while to load. To reduce the amount of data, try filtering the table."
              />
            </Delayed>
          )}
        </section>
      </div>
    );
  }
}
