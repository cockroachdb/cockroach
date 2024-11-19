// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import "./styles.styl";

interface IChipProps {
  title: React.ReactChild;
  type?:
    | "green"
    | "lightgreen"
    | "grey"
    | "blue"
    | "lightblue"
    | "yellow"
    | "red"
    | "white";
}

export const Chip: React.SFC<IChipProps> = ({ title, type }) => (
  <span className={`Chip Chip--${type}`}>{title}</span>
);
