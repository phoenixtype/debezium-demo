# MySQL Commands

- `mysql -uroot -proot -h localhost` - to enter into the MySQL shell in the docker terminal

- `USE customerdb;` - select the DB

- `CREATE TABLE customer
  (
  id INTEGER NOT NULL,
  fullname VARCHAR(255),
  email VARCHAR(255),
  CONSTRAINT customer_pkey PRIMARY KEY (id)
  );` - create a table

- `SHOW TABLES;` - to show the tables in a particular DB

- `INSERT INTO customerdb.customer (id, fullname, email) VALUES (1, 'John Doe', 'jd@example.com');` - add a row to the database

- `SELECT * FROM customerdb.customer;` - view the database

- `UPDATE customerdb.customer t SET t.email = 'john.doe@example.com' WHERE t.id = 1;` - to update the record

- `DELETE FROM customerdb.customer WHERE id = 1;` - to delete the record
