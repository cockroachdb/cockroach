CREATE TABLE second (
  i INT4 PRIMARY KEY,
  k INT4,
  UNIQUE INDEX ik (i, k),
  INDEX ki (k, i),
  CONSTRAINT second_ibfk_1 FOREIGN KEY (k) REFERENCES simple (i) ON UPDATE CASCADE
)
