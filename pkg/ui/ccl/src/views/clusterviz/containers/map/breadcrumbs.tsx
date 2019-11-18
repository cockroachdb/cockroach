// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import _ from "lodash";
import { Link } from "react-router";

import { generateLocalityRoute } from "src/util/localities";
import { LocalityTier } from "src/redux/localities";
import { intersperse } from "src/util/intersperse";
import { getLocalityLabel } from "src/util/localities";
import { MAIN_BLUE } from "src/views/shared/colors";
import mapPinIcon from "!!raw-loader!assets/mapPin.svg";
import { trustIcon } from "src/util/trust";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";

interface BreadcrumbsProps {
  tiers: LocalityTier[];
}

export class Breadcrumbs extends React.Component<BreadcrumbsProps> {
  render() {
    const paths = breadcrumbPaths(this.props.tiers);

    return (
      <div style={{ textTransform: "uppercase", fontWeight: "bold", letterSpacing: 1 }}>
        <span
          dangerouslySetInnerHTML={trustIcon(mapPinIcon)}
          style={{ paddingRight: 5, position: "relative", top: 2 }}
        />
        {intersperse(
          paths.map((path, idx) => (
            <span>
              {idx === paths.length - 1
                ? getLocalityLabel(path)
                : <Link
                    to={CLUSTERVIZ_ROOT + generateLocalityRoute(path)}
                    style={{ color: MAIN_BLUE, textDecoration: "none" }}
                  >
                    {getLocalityLabel(path)}
                  </Link>
              }
            </span>
          )),
          <span className="breadcrumb-sep"> &gt; </span>,
        )}
      </div>
    );
  }
}

function breadcrumbPaths(path: LocalityTier[]): LocalityTier[][] {
  const pathSoFar: LocalityTier[] = [];
  const output: LocalityTier[][] = [[]];
  path.forEach((tier) => {
    pathSoFar.push(tier);
    output.push(_.clone(pathSoFar));
  });
  return output;
}
