// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { InlineAlert } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useEffect, useState } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { getSchedules } from "src/api/schedulesApi";
import { commonStyles } from "src/common";
import { Delayed } from "src/delayed";
import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SortSetting } from "src/sortedtable";
import { syncHistory, useSwrWithClusterId } from "src/util";

import styles from "../schedules.module.scss";

import { statusOptions, showOptions } from "./scheduleOptions";
import { ScheduleTable } from "./scheduleTable";

const cx = classNames.bind(styles);

export interface SchedulesPageStateProps {
  sort: SortSetting;
}

export interface SchedulesPageDispatchProps {
  setSort: (value: SortSetting) => void;
}

export type SchedulesPageProps = SchedulesPageStateProps &
  SchedulesPageDispatchProps &
  RouteComponentProps;

export const SchedulesPage: React.FC<SchedulesPageProps> = props => {
  const { history, sort, setSort } = props;
  const searchParams = new URLSearchParams(history.location.search);

  // Sort Settings.
  const ascending = (searchParams.get("ascending") || undefined) === "true";
  const columnTitle = searchParams.get("columnTitle") || "";

  useEffect(() => {
    if (!columnTitle) {
      return;
    }
    setSort({ columnTitle, ascending });
  }, [setSort, columnTitle, ascending]);

  const [status, setStatus] = useState(statusOptions[0].value);
  const [show, setShow] = useState(showOptions[0].value);

  // Filter Status.
  const paramStatus = searchParams.get("status");
  useEffect(() => {
    if (
      !paramStatus ||
      statusOptions.find(option => option["value"] === paramStatus) ===
        undefined
    ) {
      return;
    }
    setStatus(paramStatus);
  }, [paramStatus, setStatus]);

  // Filter Show.
  const paramShow = searchParams.get("show");
  useEffect(() => {
    if (
      !paramShow ||
      showOptions.find(option => option["value"] === paramShow) === undefined
    ) {
      return;
    }
    setShow(paramShow);
  }, [paramShow, setShow]);

  const limit = parseInt(show) ?? 0;
  const { data, error, isLoading } = useSwrWithClusterId(
    {
      name: "schedules",
      status,
      limit,
    },
    () => getSchedules({ status, limit }),
  );
  const onStatusSelected = (item: string) => {
    setStatus(item);
    syncHistory(
      {
        status: item,
      },
      history,
    );
  };

  const onShowSelected = (item: string) => {
    setShow(item);
    syncHistory(
      {
        show: item,
      },
      history,
    );
  };

  const changeSortSetting = (ss: SortSetting): void => {
    setSort(ss);
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      history,
    );
  };

  return (
    <div className={cx("schedules-page")}>
      <Helmet title="Schedules" />
      <h3 className={commonStyles("base-heading")}>Schedules</h3>
      <div>
        <PageConfig>
          <PageConfigItem>
            <Dropdown items={statusOptions} onChange={onStatusSelected}>
              Status:{" "}
              {
                (
                  statusOptions.find(option => option["value"] === status) ??
                  statusOptions[0]
                ).name
              }
            </Dropdown>
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown items={showOptions} onChange={onShowSelected}>
              Show:{" "}
              {
                (
                  showOptions.find(option => option["value"] === show) ??
                  showOptions[0]
                ).name
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
              isUsedFilter={!!status}
              schedules={data}
              setSort={changeSortSetting}
              sort={sort}
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
};
