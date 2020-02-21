// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import "./styles.styl";

interface ISummaryCardProps {
  children: React.ReactNode;
  className?: string;
}

// tslint:disable-next-line: variable-name
export const SummaryCard: React.FC<ISummaryCardProps> = ({ children, className }) => (
  <div className={`summary--card ${className}`}>
    {children}
  </div>
);

SummaryCard.defaultProps = {
  className: "",
};
