const custom = require("../webpack.app.js");

const appConfig = custom({dist: "ccl"}, {mode: "development"});

module.exports = async ({ config, mode }) => {
  return {
    ...config,
    resolve: {
      ...config.resolve,
      modules: appConfig.resolve.modules,
      extensions: appConfig.resolve.extensions,
    },
    module: {
      rules: [
        ...appConfig.module.rules,
      ]
    },
    plugins: [
      ...config.plugins,
      // Import 'vendors' library only
      appConfig.plugins[2]
    ]
  };
};
