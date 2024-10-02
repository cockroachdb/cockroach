// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createAction } from "@reduxjs/toolkit";

import { DOMAIN_NAME } from "./utils";

export const rootActions = {
  resetState: createAction(`${DOMAIN_NAME}/RESET_STATE`),
};
