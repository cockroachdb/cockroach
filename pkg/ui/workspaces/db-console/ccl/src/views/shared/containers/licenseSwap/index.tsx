// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import omit from "lodash/omit";
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
export default function swapByLicense<
  OSSProps,
  CCLProps,
  TProps = OSSProps | CCLProps,
>(
  OSSComponent: React.ComponentClass<OSSProps>,
  CCLComponent: React.ComponentClass<CCLProps>,
) {
  const ossName = getComponentName(OSSComponent);
  const cclName = getComponentName(CCLComponent);

  return connect<OwnProps, null, TProps, AdminUIState>(mapStateToProps)(
    class extends React.Component<TProps & OwnProps & any> {
      public static displayName = `LicenseSwap(${combineNames(
        ossName,
        cclName,
      )})`;

      render() {
        const props = omit(this.props, ["enterpriseEnabled"]);

        if (!this.props.enterpriseEnabled) {
          return <OSSComponent {...(props as OSSProps)} />;
        }
        return <CCLComponent {...(props as CCLProps)} />;
      }
    },
  );
}
