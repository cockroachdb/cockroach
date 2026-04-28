// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AdminUIState } from "src/redux/state";
import { getDataFromServer } from "src/util/dataFromServer";

export function selectEnterpriseEnabled(_state: AdminUIState) {
  const { LicenseType } = getDataFromServer();
  return LicenseType !== "OSS";
}
