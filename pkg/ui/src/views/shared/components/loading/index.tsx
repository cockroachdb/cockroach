import React from "react";

interface LoadingProps {
  loading: boolean;
  className: string;
  image: string;
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
  // The wrapper <div> in the return clause is required so that this component
  // can take a list of elements instead of only a single one.
  // This is fixed in react 16, see:
  // https://reactjs.org/blog/2017/11/28/react-v16.2.0-fragment-support.html
  return (
    <div>
      {props.children}
    </div>
  );
}
