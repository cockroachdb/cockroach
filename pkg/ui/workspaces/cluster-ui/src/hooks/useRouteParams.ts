// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useParams } from "react-router-dom";

import { databaseIDAttr, tableIdAttr } from "src/util";

type Params = {
  [databaseIDAttr]: string;
  [tableIdAttr]: string;
};
export const useRouteParams = useParams<Params>;
