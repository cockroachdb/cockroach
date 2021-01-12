// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import "./titleWithIcon.styl";

interface TitleWithIconProps {
  title: string;
  src: string;
}

const TitleWithIcon: React.FC<TitleWithIconProps> = ({ title, src }) => (
  <h2 className="base-heading title-with-icon">
    <img src={src} alt="Stack" />
    {title}
  </h2>
);

export default TitleWithIcon;
