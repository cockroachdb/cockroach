// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

declare module "*.module.scss" {
  const content: { [className: string]: string };
  export default content;
}
declare module "*.png";
declare module "*.gif";
declare module "*.scss";
declare module "*.svg";

type ConstructorType = new (...args: any) => any;

type FirstConstructorParameter<P extends ConstructorType> =
  ConstructorParameters<P>[0];

type Tuple<T> = [T, T];

type Dictionary<V> = {
  [key: string]: V;
};

function compose(): <R>(a: R) => R;

// Extend Window interface with possible Redux DevTools extension.
interface Window {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  __REDUX_DEVTOOLS_EXTENSION__?: typeof compose;
}

declare module "highlight.js/lib/core";
declare module "highlight.js/lib/languages/pgsql";
