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
        { pattern: 'tsconfig.json' },
        { pattern: 'typings/**/*.d.ts' },
      ],
    },

    browsers: ['PhantomJS'],

    singleRun: true,
  })
}
