create table if not exists customers (
    customerid serial primary key,
    firstname varchar(100) not null,
    lastname varchar(100) not null,
    email varchar(255) not null unique
);

create table if not exists products (
    productid serial primary key,
    productname varchar(255) not null,
    price numeric(10, 2) not null check (price >= 0)
);

create table if not exists orders (
    orderid serial primary key,
    customerid int not null references customers(customerid),
    orderdate timestamp not null default now(),
    totalamount numeric(12, 2) not null default 0
);

create table if not exists orderitems (
    orderitemid serial primary key,
    orderid int not null references orders(orderid) on delete cascade,
    productid int not null references products(productid),
    quantity int not null check (quantity > 0),
    subtotal numeric(12, 2) not null check (subtotal >= 0)
);

insert into customers (firstname, lastname, email)
values
    ('Ivan', 'Petrov', 'ivan.petrov@example.com'),
    ('Anna', 'Sidorova', 'anna.sidorova@example.com')
on conflict (email) do nothing;

insert into products (productname, price)
values
    ('Laptop', 1200.00),
    ('Mouse', 25.50),
    ('Keyboard', 80.00)
on conflict do nothing;
