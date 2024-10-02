// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const path = require("path");
const appConfig = require("../webpack.config");

const customConfig = appConfig();

module.exports = {
  stories: ["../src/**/*.stories.tsx"],
  addons: ["@storybook/addon-actions", "@storybook/addon-links"],
  typescript: { reactDocgen: false },
  webpackFinal: async config => {
    config.module.rules = [...customConfig.module.rules];
    config.resolve.extensions.push(".ts", ".tsx");
    config.resolve.alias.src = path.resolve(__dirname, "../src");
    return config;
  },
  framework: "@storybook/react",
};
