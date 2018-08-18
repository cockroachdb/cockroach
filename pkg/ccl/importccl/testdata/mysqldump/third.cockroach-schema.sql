CREATE TABLE third (
    i INT PRIMARY KEY,
    a INT,
    b INT,
    c INT,
    INDEX a (a, b),
    INDEX c (c),
    FOREIGN KEY (a, b) REFERENCES second (i, k),
    FOREIGN KEY (c) REFERENCES third (i) ON UPDATE CASCADE
);