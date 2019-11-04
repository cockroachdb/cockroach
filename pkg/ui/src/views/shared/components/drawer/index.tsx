import React from "react";
import { Drawer, Button, Divider } from "antd";
import { Link } from "react-router";

interface IDrawerProps {
  visible: boolean;
  onClose: () => void;
  details?: boolean;
  children?: React.ReactNode | string;
  data: any;
}

const openDetails = (data: any) => {
  const base = data.app && data.app.length > 0 ? `/statements/${data.app}/${data.implicitTxn}` : `/statement/${data.implicitTxn}`;
  const link = `${base}/${encodeURIComponent(data.statement)}`;
  return (
    <Link to={link}>View statement details</Link>
  );
};

// tslint:disable-next-line: variable-name
export const DrawerComponent = ({ visible, onClose, children, data, details }: IDrawerProps) => (
  <Drawer
    title={
      <div className="__actions">
        <Button type="link" ghost block onClick={onClose}>
          Close
        </Button>
        {details && (
          <React.Fragment>
            <Divider type="vertical" />
            {openDetails(data)}
          </React.Fragment>
        )}
      </div>
    }
    placement="bottom"
    closable={false}
    onClose={onClose}
    visible={visible}
    className="drawer--preset-black"
    getContainer={false}
  >
    {children}
  </Drawer>
);
