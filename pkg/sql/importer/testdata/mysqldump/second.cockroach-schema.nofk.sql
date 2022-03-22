CREATE TABLE second (
  i INT PRIMARY KEY,
  k INT,
  INDEX ik (i, k),
  INDEX ki (k, i)
)
