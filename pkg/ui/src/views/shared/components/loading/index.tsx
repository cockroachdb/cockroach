import React from "react";

interface LoadingProps {
  loading: boolean;
  className: string;
  image: string;
  render: () => React.ReactNode;
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
  return (
    <React.Fragment>
      { props.render() }
    </React.Fragment>
  );
}
