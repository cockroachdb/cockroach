import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import common_en from "./translations/en/common.json";

const resources = {
  en: {
    common: common_en,
  },
};

const options = {
  lng: "en",
  debug: false,
  resources,
  interpolation: { escapeValue: false },
};

i18n
  .use(initReactI18next)
  .init(options);

export default i18n;
