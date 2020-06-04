// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
const path = require('path');
const requireOpt = require("./requireOptModule");
const wp = requireOpt("@cypress/webpack-preprocessor");

const webpackOptions = {
  resolve: {
    extensions: ['.ts', '.js'],
    modules: [
      requireOpt.OPT_NODE_MODULES_PATH,
    ],
    alias: {
      "src": path.resolve('src/')
    }
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        exclude: [/node_modules/],
        use: [
          {
            loader: 'ts-loader'
          }
        ]
      }
    ]
  }
};

const options = {
  webpackOptions
};

module.exports = wp(options);
