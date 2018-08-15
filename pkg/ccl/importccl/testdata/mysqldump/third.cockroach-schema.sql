CREATE TABLE third (
    i INT PRIMARY KEY,
    a INT,
    b INT,
    "C" INT,
    INDEX a (a, b),
    INDEX "C" ("C"),
    FOREIGN KEY (a, b) REFERENCES second (i, k),
    FOREIGN KEY (c) REFERENCES third (i) ON UPDATE CASCADE
);