// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";

import { selectEnterpriseEnabled } from "src/redux/license";
import { AdminUIState } from "src/redux/state";

// Some of the type magic is adapted from @types/react-redux.
// Some of the code is adapted from react-redux.

type ComponentClass<P> = React.ComponentClass<P>;
type StatelessComponent<P> = React.StatelessComponent<P>;
type Component<P> = ComponentClass<P> | StatelessComponent<P>;

function getComponentName<P>(wrappedComponent: Component<P>) {
  return wrappedComponent.displayName || wrappedComponent.name || "Component";
}

function combineNames(a: string, b: string) {
  if (a === b) {
    return a;
  }

  return a + "," + b;
}

interface OwnProps {
  enterpriseEnabled: boolean;
}

function mapStateToProps(state: AdminUIState): OwnProps {
  return {
    enterpriseEnabled: selectEnterpriseEnabled(state),
  };
}

/**
 * LicenseSwap is a higher-order component that swaps out two components based
 * on the current license status.
 */
export default function swapByLicense<TProps>(
  OSSComponent: React.ComponentClass<TProps>,
  CCLComponent: React.ComponentClass<TProps>,
) {
  const ossName = getComponentName(OSSComponent);
  const cclName = getComponentName(CCLComponent);

  class LicenseSwap extends React.Component<TProps & OwnProps & any> {
    public static displayName = `LicenseSwap(${combineNames(
      ossName,
      cclName,
    )})`;

    render() {
      const props = _.omit(this.props, ["enterpriseEnabled"]);

      if (!this.props.enterpriseEnabled) {
        return <OSSComponent {...(props as TProps)} />;
      }
      return <CCLComponent {...(props as TProps)} />;
    }
  }

  return connect<OwnProps, null, TProps, AdminUIState>(mapStateToProps)(
    LicenseSwap,
  );
}
