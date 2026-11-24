// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "src/js/protos";

export type ILocation =
  protos.cockroach.server.serverpb.LocationsResponse.ILocation;

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation;
  };
}
