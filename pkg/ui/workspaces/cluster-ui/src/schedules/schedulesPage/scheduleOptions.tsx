// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
export const SCHEDULE_STATUS_ACTIVE = "ACTIVE";
export const SCHEDULE_STATUS_PAUSED = "PAUSED";

export const statusOptions = [
  { value: "", name: "All" },
  { value: SCHEDULE_STATUS_ACTIVE, name: "Active" },
  { value: SCHEDULE_STATUS_PAUSED, name: "Paused" },
];

export const showOptions = [
  { value: "50", name: "Latest 50" },
  { value: "0", name: "All" },
];
