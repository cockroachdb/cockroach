import React from "react";
import _ from "lodash";
import { Link } from "react-router";

import { generateLocalityRoute } from "src/util/localities";
import { LocalityTier } from "src/redux/localities";
import { intersperse } from "src/util/intersperse";
import { getLocalityLabel } from "src/util/localities";

interface BreadcrumbsProps {
  tiers: LocalityTier[];
}

export class Breadcrumbs extends React.Component<BreadcrumbsProps> {
  render() {
    const paths = breadcrumbPaths(this.props.tiers);

    return (
      <div className="breadcrumbs">
        {intersperse(
          paths.map((path) => (
            <span className="breadcrumb">
              <Link to={"/clusterviz/" + generateLocalityRoute(path)}>
                {getLocalityLabel(path)}
              </Link>
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
