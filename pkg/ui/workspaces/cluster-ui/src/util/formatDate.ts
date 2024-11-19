// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Moment } from "moment-timezone";

export const dateFormat = "Y-MM-DD HH:mm:ss";

export function formatDate(time: Moment): string {
  return time.format(dateFormat);
}
