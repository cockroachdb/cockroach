// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useParams } from "react-router";

import { databaseIDAttr } from "src/util";

type Params = {
  [databaseIDAttr]: string;
  // Add more as needed.
};
export const useRouteParams = useParams<Params>;
