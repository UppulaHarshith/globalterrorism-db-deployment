CREATE TABLE country (
countryid int Not null unique,
countrycode INT,
country VARCHAR(50),
provstate VARCHAR(50),
city VARCHAR(50),
PRIMARY KEY (countryid));