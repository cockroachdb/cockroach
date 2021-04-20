const path = require("path");
const appConfig = require("../webpack.config");

module.exports = {
  stories: ['../src/**/*.stories.tsx'],
  addons: ['@storybook/addon-actions', '@storybook/addon-links'],
  webpackFinal: async config => {
    config.module.rules = [
      {
        test: /\.(ts|tsx)$/,
        loader: require.resolve('babel-loader'),
        options: {
          presets: [['react-app', { flow: false, typescript: true }]],
        },
      },
      ...appConfig.module.rules,
    ]
    config.resolve.extensions.push('.ts', '.tsx');
    config.resolve.alias.src = path.resolve(__dirname, "../src")
    return config;
  },
};
