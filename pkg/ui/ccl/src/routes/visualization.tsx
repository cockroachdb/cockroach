// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
import React from "react";
import { Route } from "react-router";
import ClusterViz from "src/views/clusterviz/containers/map";

export default function(): JSX.Element {
  return <Route path="clusterviz" component={ClusterViz} />;
}
