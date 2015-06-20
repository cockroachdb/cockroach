SELECT !8 FROM t#syntax error at position 15 near FROM
SELECT $ FROM t#syntax error at position 9 near $
SELECT : FROM t#syntax error at position 9 near :
SELECT 078 FROM t#syntax error at position 11 near 078
SELECT 'aa\#syntax error at position 12 near aa
SELECT 'aa#syntax error at position 12 near aa
SELECT /* aa#syntax error at position 13 near /* aa
CREATE TABLE a (b INT2)#syntax error at position 23 near INT2
CREATE TABLE a (b INT UNIQUE NULL)#syntax error at position 34 near NULL
CREATE TABLE a (b INT UNIQUE NOT NULL)#syntax error at position 33 near NOT
CREATE TABLE a (b INT UNIQUE PRIMARY KEY)#syntax error at position 37 near PRIMARY
