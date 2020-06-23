const presets = [
  [
    "@babel/env",
    {
      modules: false,
    },
  ],
  "@babel/react",
  "@babel/typescript",
];
const plugins = [
  "@babel/proposal-class-properties",
  "@babel/proposal-object-rest-spread",
];

module.exports = { presets, plugins };
