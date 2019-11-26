// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { Helmet } from "react-helmet";

export interface NodeHistoryProps {

}

export default function NodeHistory(_props: NodeHistoryProps) {
  return (
    <section className="section">
      <Helmet>
        <title>Decommissioned Node History | Debug</title>
      </Helmet>
      <h1>Decommissioned Node History</h1>
    </section>
  );
}
