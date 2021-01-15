import React from "react";
import classnames from "classnames/bind";
import styles from "./emptyPanel.module.scss";
import { Text, TextTypes } from "../../text";
import { Anchor } from "../../anchor";
import { Button } from "../../button";
import heroBannerLp from "../../assets/heroBannerLp.png";

const cx = classnames.bind(styles);

interface IMainEmptyProps {
  title?: string;
  description?: string;
  label?: React.ReactNode;
  link?: string;
  anchor?: string;
  backgroundImage?: string;
}

type OnClickXORHref =
  | {
      onClick?: () => void;
      buttonHref?: never;
    }
  | {
      onClick?: never;
      buttonHref?: string;
    };

export type EmptyPanelProps = OnClickXORHref & IMainEmptyProps;

export const EmptyPanel: React.FC<EmptyPanelProps> = ({
  title = "No results",
  description,
  anchor = "Learn more",
  label = "Learn more",
  link,
  backgroundImage = heroBannerLp,
  onClick,
  buttonHref,
}) => (
  <div
    className={cx("cl-empty-view")}
    style={{ backgroundImage: `url(${backgroundImage})` }}
  >
    <Text className={cx("cl-empty-view__title")} textType={TextTypes.Heading3}>
      {title}
    </Text>
    <div className={cx("cl-empty-view__content")}>
      <main className={cx("cl-empty-view__main")}>
        <Text
          textType={TextTypes.Body}
          className={cx("cl-empty-view__main--text")}
        >
          {description}
          {link && (
            <Anchor href={link} className={cx("cl-empty-view__main--anchor")}>
              {anchor}
            </Anchor>
          )}
        </Text>
      </main>
      <footer className={cx("cl-empty-view__footer")}>
        <Button
          type="primary"
          onClick={() =>
            buttonHref ? window.open(buttonHref) : onClick && onClick()
          }
        >
          {label}
        </Button>
      </footer>
    </div>
  </div>
);
