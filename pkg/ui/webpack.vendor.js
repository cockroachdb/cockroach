// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

"use strict";

const path = require("path");
const webpack = require("webpack");

const pkg = require("./package.json");

const prodDependencies = Object.keys(pkg.dependencies);

// tslint:disable:object-literal-sort-keys
module.exports = {
  entry: {
    vendor: prodDependencies,
  },

  output: {
    filename: "vendor.oss.dll.js",
    path: path.resolve(__dirname, "dist"),
    library: "[name]_[hash]",
  },

  resolve: {
    modules: [path.resolve(__dirname, "node_modules")],
  },

  module: {
    rules: [
      {
        test: /\.js$/,
        // analytics-node is an es6 module and we must run it through babel. As
        // of 8/25/2017 there appears to be no better way to run babel only on
        // the specific packages in node_modules that actually use ES6.
        // https://github.com/babel/babel-loader/issues/171
        exclude: /node_modules\/(?!analytics-node)/,
        use: ["cache-loader", "thread-loader", "babel-loader"],
      },
    ],
  },

  plugins: [
    new webpack.DllPlugin({
      name: "[name]_[hash]",
      path: path.resolve(__dirname, "vendor.oss.manifest.json"),
    }),
  ],
};
