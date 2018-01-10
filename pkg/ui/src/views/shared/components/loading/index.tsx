import React from "react";

interface LoadingProps {
  loading: boolean;
  className: string;
  image?: string;
  text?: string;
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
  if (props.loading && !props.text) {
    return <div className={props.className} style={image} />;
  }
  if (props.loading && props.text) {
    return <div className={props.className}><h2>{props.text}</h2></div>;
  }
  return props.children as JSX.Element;
}
