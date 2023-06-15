// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// isSystemTenant checks whether the provided tenant name is the
// system tenant.
import { SYSTEM_TENANT_NAME } from "./cookies";

export const isSystemTenant = (tenantName: string): boolean => {
  return tenantName === SYSTEM_TENANT_NAME;
};
