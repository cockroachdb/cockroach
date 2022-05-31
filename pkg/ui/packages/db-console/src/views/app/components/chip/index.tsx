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
import "./styles.styl";

interface IChipProps {
  title: string;
  type?: "green" | "lightgreen" | "grey" | "blue" | "lightblue" | "yellow";
}

export const Chip: React.SFC<IChipProps> = ({ title, type }) => (
  <span className={`Chip Chip--${type}`}>{title}</span>
);
