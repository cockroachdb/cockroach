USE mydb;

CREATE TABLE bello (
                     id INT UNIQUE,
                     name varchar(12)
);

INSERT INTO bello (id, name) values (1, 'a'), (2, 'b'), (3, 'c');
