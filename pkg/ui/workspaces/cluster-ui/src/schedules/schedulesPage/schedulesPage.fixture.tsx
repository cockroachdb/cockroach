// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import Long from "long";
import moment from "moment-timezone";

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
