// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment, { Moment } from "moment";

export const start = moment.utc("2021.08.08");
export const end = moment.utc("2021.08.12");

export const allowedInterval: [Moment, Moment] = [start, end];
