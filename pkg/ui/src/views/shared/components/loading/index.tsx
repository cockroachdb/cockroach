import React from "react";

interface LoadingProps {
  loading: boolean;
  className: string;
  image: string;
  // The render function should probably be the only API, but currently
  // it will be used if it is there, and fall back to children if it is not.
  // TODO(vilterp): migrate all usages of Loading to use render prop.
  render?: () => JSX.Element;
  children?: React.ReactNode;
}

// *
// * Loading will display a background image instead of the content if the
// * loading prop is true.
// *
export default function Loading(props: LoadingProps) {
  const image = {
    "backgroundImage": `url(${props.image})`,
  };
  if (props.loading) {
    return <div className={props.className} style={image} />;
  }
  if (props.render) {
    return props.render();
  }

  // This throws an error if more than one child is passed.
  // Unfortunately the error seems to get eaten by some try/catch
  // above this, but leaving it here to at least signal intent.
  // Also unfortunately it's unclear how to enforce this invariant
  // with the type system, since the `children` argument matches
  // both one node and multiple nodes.
  return React.Children.only(props.children);
}
