CREATE TABLE customers(
UserName Varchar(30) PRIMARY KEY NOT NULL,
Email Varchar(30),
JoinDate DATE
);

CREATE TABLE products(
ProductID Varchar(15) PRIMARY KEY NOT NULL,
Title Varchar(50),
Stock INTEGER,
Price DECIMAL(20, 2)
);

CREATE TABLE reviews(
UserName Varchar(30) NOT NULL,
ProductID Varchar(15) NOT NULL,
Email Varchar(30),
JoinDate DATE,
Title Varchar(50),
Stock INTEGER,
Price DECIMAL(20, 2),
commentTitle Varchar(30),
commentDate Timestamp,
commentContent TEXT
);