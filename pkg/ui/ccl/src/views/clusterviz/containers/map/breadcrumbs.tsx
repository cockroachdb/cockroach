import React from "react";
import _ from "lodash";
import Link from "react-router/lib/Link";

import { generateLocalityRoute } from "oss/src/util/localities";
import { LocalityTier } from "src/redux/localities";
import { intersperse } from "src/util/intersperse";

interface BreadcrumbsProps {
  tiers: LocalityTier[];
}

export class Breadcrumbs extends React.Component<BreadcrumbsProps, {}> {
  getLabel(path: LocalityTier[]): string {
    if (path.length === 0) {
      return "Global";
    }

    const thisTier = path[path.length - 1];
    return `${thisTier.key}=${thisTier.value}`;
  }

  render() {
    const paths = breadcrumbPaths(this.props.tiers);

    return (
      <div className="breadcrumbs">
        {intersperse(
            paths.map((path) => {
          return (
            <span className="breadcrumb">
            <Link to={"/clusterviz/" + generateLocalityRoute(path)}>
              {this.getLabel(path)}
          </Link>
          </span>
        );
        }),
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
