// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const path = require("path");
const appConfig = require("../webpack.config");

module.exports = {
  stories: ['../src/**/*.stories.tsx'],
  addons: ['@storybook/addon-actions', '@storybook/addon-links'],
  webpackFinal: async config => {
    config.module.rules = [
      ...appConfig.module.rules,
    ]
    config.resolve.extensions.push('.ts', '.tsx');
    config.resolve.alias.src = path.resolve(__dirname, "../src")
    return config;
  },
};
