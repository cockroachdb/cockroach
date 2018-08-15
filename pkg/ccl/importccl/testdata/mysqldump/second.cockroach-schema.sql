CREATE TABLE second (
  i INT PRIMARY KEY,
  k INT,
  UNIQUE INDEX ik (i, k),
  INDEX ki (k, i),
  CONSTRAINT second_ibfk_1 FOREIGN KEY (k) REFERENCES simple (i) ON UPDATE CASCADE
)
