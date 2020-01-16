// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import "./styles.styl";

interface ISummaryCardProps {
  children: React.ReactNode;
}

// tslint:disable-next-line: variable-name
export const SummaryCard = ({ children }: ISummaryCardProps) => (
  <div className="summary--card">
    {children}
  </div>
);