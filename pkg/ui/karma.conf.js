// Karma configuration
// Generated on Wed Mar 22 2017 16:39:26 GMT-0400 (EDT)

'use strict';

const webpackConfig = require('./webpack.config');

module.exports = function(config) {
  config.set({

    // base path that will be used to resolve all patterns (eg. files, exclude)
    basePath: '',


    // frameworks to use
    // available frameworks: https://npmjs.org/browse/keyword/karma-adapter
    frameworks: ['mocha', 'chai', 'sinon'],


    // list of files / patterns to load in the browser
    files: [
      'src/**/*.spec.*'
    ],


    // TODO(tamird): https://github.com/webpack-contrib/karma-webpack/issues/188.
    mime: {
      'text/x-typescript': ['ts','tsx'],
    },


    // preprocess matching files before serving them to the browser
    // available preprocessors: https://npmjs.org/browse/keyword/karma-preprocessor
    preprocessors: {
      'src/**/*.spec.*': ['webpack', 'sourcemap'],
    },


    // https://github.com/airbnb/enzyme/blob/master/docs/guides/webpack.md
    webpack: Object.assign(webpackConfig, {
      devtool: 'source-map',
      externals: {
        'react/addons': true,
        'react/lib/ExecutionEnvironment': true,
        'react/lib/ReactContext': true,
      },
    }),


    // test results reporter to use
    // possible values: 'dots', 'progress'
    // available reporters: https://npmjs.org/browse/keyword/karma-reporter
    reporters: ['progress'],


    // web server port
    port: 9876,


    // enable / disable colors in the output (reporters and logs)
    colors: true,


    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_INFO,


    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: false,


    // start these browsers
    // available browser launchers: https://npmjs.org/browse/keyword/karma-launcher
    browsers: ['jsdom'],


    // Continuous Integration mode
    // if true, Karma captures browsers, runs the tests and exits
    singleRun: true,

    // Concurrency level
    // how many browser should be started simultaneous
    concurrency: Infinity
  })
}
