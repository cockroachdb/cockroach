'use strict';

module.exports = function (config) {
  config.set({
    frameworks: [
      'jspm', 'mocha',
    ],

    jspm: {
      loadFiles: [
        'app/**/*.spec.ts*',
      ],
      serveFiles: [
        { pattern: 'app/**/*' },
        { pattern: 'tsconfig.build.json' },
        { pattern: 'typings/**/*.d.ts' },
      ],
    },

    browsers: ['jsdom'],

    singleRun: true,

    browserNoActivityTimeout: 60000,
  })
}
