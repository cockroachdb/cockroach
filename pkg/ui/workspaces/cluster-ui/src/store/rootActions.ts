// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DOMAIN_NAME } from "./utils";
import { createAction } from "@reduxjs/toolkit";

export const rootActions = {
  resetState: createAction(`${DOMAIN_NAME}/RESET_STATE`),
};
