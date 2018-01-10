// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

// Some of the type magic is adapted from @types/react-redux.
// Some of the code is adapted from react-redux.

type ComponentClass<P> = React.ComponentClass<P>;
type StatelessComponent<P> = React.StatelessComponent<P>;
type Component<P> = ComponentClass<P> | StatelessComponent<P>;

function getComponentName<P>(wrappedComponent: Component<P>) {
  return wrappedComponent.displayName
    || wrappedComponent.name
    || "Component";
}

function combineNames(a: string, b: string) {
  if (a === b) {
    return a;
  }

  return a + "," + b;
}

/**
 * LicenseSwap is a higher-order component that swaps out two components based
 * on the current license status.
 */
export default function swapByLicense<TProps>(
  // tslint:disable:variable-name
  OSSComponent: Component<TProps>,
  CCLComponent: Component<TProps>,
  // tslint:enable:variable-name
) {
  const ossName = getComponentName(OSSComponent);
  const cclName = getComponentName(CCLComponent);

  class LicenseSwap extends React.Component<TProps, {}> {
    public static displayName = `LicenseSwap(${combineNames(ossName, cclName)})`;

    render() {
      if (location.hash.indexOf("pretend-license-expired") !== -1) {
        return <OSSComponent {...this.props} />;
      }
      return <CCLComponent {...this.props} />;
    }
  }

  return LicenseSwap;
}
