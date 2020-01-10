// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import NoResults from "assets/no-results.png";
import "./styles.styl";

interface IEmptyProps {
  description?: string;
}

// tslint:disable-next-line: variable-name
export const Empty: React.SFC <IEmptyProps> = ({ description = "No Data" }) => (
  <div className="Empty">
    <img src={NoResults} />
    <p className="Empty__label">{description}</p>
  </div>
);
