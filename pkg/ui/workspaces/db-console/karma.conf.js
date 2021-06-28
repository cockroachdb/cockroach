// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Karma configuration
// Generated on Wed Mar 22 2017 16:39:26 GMT-0400 (EDT)

"use strict";

const webpackConfig = require("./webpack.app")({dist: "ccl"}, {mode: "development"});

module.exports = function(config) {
  config.set({
    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: false,

    // base path that will be used to resolve all patterns (eg. files, exclude)
    basePath: "",

    // redirect `console.log`s in test code to Karma's stdout.
    // This is the default behavior in Karma 2.0; so we can remove when we upgrade.
    browserConsoleLogOptions: {
      format: "%b %T: %m",
      level: "log",
      terminal: true,
    },

    // start these browsers
    // available browser launchers: https://npmjs.org/browse/keyword/karma-launcher
    browsers: ["ChromeHeadless"],

    // enable / disable colors in the output (reporters and logs)
    colors: true,

    // Concurrency level
    // how many browser should be started simultaneous
    concurrency: Infinity,

    // list of files / patterns to load in the browser
    files: [
      "src/polyfills.ts",
      "tests-loader.js",
    ],

    // frameworks to use
    // available frameworks: https://npmjs.org/browse/keyword/karma-adapter
    frameworks: ["mocha", "chai", "sinon"],

    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_INFO,

    // TODO(tamird): https://github.com/webpack-contrib/karma-webpack/issues/188.
    mime: {
      "text/x-typescript": ["ts", "tsx"],
    },

    // web server port
    port: 9876,

    // preprocess matching files before serving them to the browser
    // available preprocessors: https://npmjs.org/browse/keyword/karma-preprocessor
    preprocessors: {
      "src/polyfills.ts": ["webpack"],
      "tests-loader.js": ["webpack", "sourcemap"],
    },

    // test results reporter to use
    // possible values: "dots", "progress"
    // available reporters: https://npmjs.org/browse/keyword/karma-reporter
    reporters: ["mocha"],

    // Continuous Integration mode
    // if true, Karma captures browsers, runs the tests and exits
    singleRun: true,

    // https://github.com/airbnb/enzyme/blob/master/docs/guides/webpack.md
    webpack: {
      devtool: "eval-cheap-source-map",
      mode: "development",
      module: webpackConfig.module,
      resolve: webpackConfig.resolve,
    },

    // "stats" needs to be copied to webpackMiddleware configuration in order
    // to correctly configure console output
    webpackMiddleware: {
      noInfo: true,
      stats: webpackConfig.stats,
    },
  });
};
