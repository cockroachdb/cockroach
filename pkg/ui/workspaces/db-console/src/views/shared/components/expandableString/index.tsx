// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import isNil from "lodash/isNil";
import React, { useState } from "react";

import { trustIcon } from "src/util/trust";

import collapseIcon from "!!raw-loader!assets/collapse.svg";
import expandIcon from "!!raw-loader!assets/expand.svg";

import "./expandable.scss";

const truncateLength = 50;

interface ExpandableStringProps {
  short?: string;
  long: string;
}

// ExpandableString displays a short string with a clickable ellipsis. When
// clicked, the component displays long. If short is not specified, it uses
// the first 50 chars of long.
export function ExpandableString({
  short,
  long,
}: ExpandableStringProps): React.ReactElement {
  const [expanded, setExpanded] = useState(false);

  const onClick = (ev: any) => {
    ev.preventDefault();
    setExpanded(prev => !prev);
  };

  const renderText = () => {
    if (expanded) {
      return (
        <span className="expandable__text expandable__text--expanded">
          {long}
        </span>
      );
    }

    const shortText = short || long.substr(0, truncateLength).trim();
    return <span className="expandable__text">{shortText}&nbsp;&hellip;</span>;
  };

  const neverCollapse = isNil(short) && long.length <= truncateLength + 2;
  if (neverCollapse) {
    return <span className="expandable__long">{long}</span>;
  }

  const icon = expanded ? collapseIcon : expandIcon;
  return (
    <div className="expandable" onClick={onClick}>
      <span className="expandable__long">{renderText()}</span>
      <span
        className="expandable__icon"
        dangerouslySetInnerHTML={trustIcon(icon)}
      />
    </div>
  );
}
