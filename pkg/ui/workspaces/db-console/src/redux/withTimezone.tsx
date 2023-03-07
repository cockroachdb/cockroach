import React from "react";

import {selectTimezoneSetting} from "src/redux/clusterSettings";
import {useSelector} from "react-redux";

function WithTimezone<T>(Component: any) {
  function ComponentWithTimezoneProp(props: T) {
    const timezone = useSelector(selectTimezoneSetting);
    return <Component timezone={timezone} {...props} />
  }
  return ComponentWithTimezoneProp;
}

export default WithTimezone;