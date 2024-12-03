// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createContext } from "react";

export type ClusterDetailsContextType = {
  isTenant?: boolean;
  clusterId?: string;
};

// This is used by CC to fill in details such as whether we have a tenant or not.
export const ClusterDetailsContext = createContext<ClusterDetailsContextType>({
  isTenant: false,
  clusterId: null,
});
