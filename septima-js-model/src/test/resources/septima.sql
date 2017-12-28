Create Table Customer (
    id integer not null,
    name varchar(200)
);
Alter Table Customer Add Constraint Customer_pk Primary Key (id);

Create Table Good (
    id integer not null,
    name varchar(200)
);
Alter Table Good Add Constraint Good_pk Primary Key (id);

Create Table GoodOrder (
    id integer  not null,
    customer_id integer not null,
    good_id integer not null,
    comment varchar(200),
    moment timestamp,
    paid boolean,
    summ double,
    destination geometry,
    bad_data uuid
);
Alter Table GoodOrder Add Constraint GoodOrder_pk Primary Key (id);
Alter Table GoodOrder Add Constraint GoodOrder_Customer_fk Foreign Key (customer_id) References Customer(id);
Alter Table GoodOrder Add Constraint GoodOrder_God_fk Foreign Key (good_id) References Good(id);
