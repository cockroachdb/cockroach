// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { createMemoryHistory } from "history";
import Long from "long";
import moment from "moment-timezone";

import { Schedule } from "src/api/schedulesApi";

import { SchedulesPageProps } from "./schedulesPage";

const schedulesTimeoutErrorMessage =
  "Unable to retrieve the Schedules table. To reduce the amount of data, try filtering the table.";

const defaultScheduleProperties = {
  owner: "root",
  created: moment("15 Aug 2022"),
  nextRun: moment("15 Aug 2122"),
  jobsRunning: 1,
  state: "doing some stuff",
  recurrence: "@weekly",
  command: "{}",
};

export const activeScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(8136728577, 70289336),
  label: "automatic SQL Stats compaction",
  owner: "node",
  status: "ACTIVE",
};

const pausedScheduleFixture = {
  ...defaultScheduleProperties,
  id: new Long(7003330561, 70312826),
  label:
    "ALTER TABLE movr.public.user_promo_codes ADD FOREIGN KEY (city, user_id) REFERENCES movr.public.users (city, id)",
  status: "PAUSED",
  error: "mock failure message",
};

export const allSchedulesFixture = [
  activeScheduleFixture,
  pausedScheduleFixture,
];

const history = createMemoryHistory({ initialEntries: ["/statements"] });

const staticScheduleProps: Omit<
  SchedulesPageProps,
  "schedules" | "schedulesError" | "schedulesLoading" | "onFilterChange"
> = {
  history,
  location: {
    pathname: "/schedules",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/schedules",
    url: "/schedules",
    isExact: true,
    params: "{}",
  },
  sort: {
    columnTitle: "creationTime",
    ascending: false,
  },
  status: "",
  show: "50",
  setSort: () => {},
  setStatus: () => {},
  setShow: () => {},
  refreshSchedules: () => null,
};

const getSchedulesPageProps = (
  schedules: Array<Schedule>,
  error: Error | null = null,
  loading = false,
): SchedulesPageProps => ({
  ...staticScheduleProps,
  schedules,
  schedulesError: error,
  schedulesLoading: loading,
});

export const withData: SchedulesPageProps =
  getSchedulesPageProps(allSchedulesFixture);
export const empty: SchedulesPageProps = getSchedulesPageProps([]);
export const loading: SchedulesPageProps = getSchedulesPageProps(
  allSchedulesFixture,
  null,
  true,
);
export const error: SchedulesPageProps = getSchedulesPageProps(
  allSchedulesFixture,
  new Error(schedulesTimeoutErrorMessage),
  false,
);
