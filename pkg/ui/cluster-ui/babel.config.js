const presets = [
  [
    "@babel/env",
    {
      modules: false,
    },
  ],
  "@babel/react",
  [
    "@babel/typescript",
    {
      allowNamespaces: true,
    }
  ],
];

const plugins = [
  "@babel/proposal-class-properties",
  "@babel/proposal-object-rest-spread",
  ["import", { "libraryName": "antd", "style": "css" }]
];

const env = {
  test: {
    plugins: ["@babel/plugin-transform-modules-commonjs"],
  }
}
module.exports = { presets, plugins, env };
