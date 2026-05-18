create table if not exists accounts (
    id int primary key,
    owner varchar(64) not null,
    balance int not null
) engine = innodb;

create table if not exists products (
    id int primary key auto_increment,
    name varchar(128) not null,
    price int not null,
    category varchar(32) not null
) engine = innodb;

insert into accounts (id, owner, balance) values
    (1, 'Alice', 1000),
    (2, 'Bob',    500);

insert into products (name, price, category) values
    ('SQL Antipatterns',         500, 'book'),
    ('Designing Data Intensive', 700, 'book'),
    ('PostgreSQL Up and Running',600, 'book'),
    ('MySQL Cookbook',           550, 'book'),
    ('Database Internals',       650, 'book');
