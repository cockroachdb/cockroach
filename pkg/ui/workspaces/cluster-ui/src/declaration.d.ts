// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

declare module "*.module.scss" {
  const content: { [className: string]: string };
  export default content;
}
declare module "*.png";
declare module "*.gif";
declare module "*.scss";
declare module "*.svg";

type ConstructorType = new (...args: any) => any;

type FirstConstructorParameter<
  P extends ConstructorType
> = ConstructorParameters<P>[0];

type Tuple<T> = [T, T];

type Dictionary<V> = {
  [key: string]: V;
};

declare module "highlight.js/lib/core";
declare module "highlight.js/lib/languages/pgsql";
