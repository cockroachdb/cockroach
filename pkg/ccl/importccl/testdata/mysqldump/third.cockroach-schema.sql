CREATE SEQUENCE third_auto_inc;

CREATE TABLE third (
    i INT4 NOT NULL DEFAULT nextval('third_auto_inc':::STRING) PRIMARY KEY,
    a INT4,
    b INT4,
    c INT4,
    INDEX a (a, b),
    INDEX c (c),
    FOREIGN KEY (a, b) REFERENCES second (i, k),
    FOREIGN KEY (c) REFERENCES third (i) ON UPDATE CASCADE
);
