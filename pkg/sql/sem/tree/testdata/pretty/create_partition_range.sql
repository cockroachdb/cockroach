CREATE TABLE students_by_range (
   id SERIAL,
   name STRING,
   email STRING,                                                                                           
   country STRING,
   expected_graduation_date DATE,                                                                                      
   PRIMARY KEY (expected_graduation_date, id))
   PARTITION BY RANGE (expected_graduation_date)
      (PARTITION graduated VALUES FROM (MINVALUE) TO ('2017-08-15'),
      PARTITION current VALUES FROM ('2017-08-15') TO (MAXVALUE))
