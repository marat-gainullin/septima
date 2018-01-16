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
    id integer not null,
    customer_id integer not null,
    seller_id integer,
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
Alter Table GoodOrder Add Constraint GoodOrder_Seller_fk Foreign Key (seller_id) References Customer(id);
Alter Table GoodOrder Add Constraint GoodOrder_God_fk Foreign Key (good_id) References Good(id);

create table UserGroups (
    userName varchar(100) not null,
    userGroup varchar(100) not null
);
Alter Table UserGroups Add Constraint UserGroups_pk Primary Key (userName, userGroup);

create table GoodsHierarchy (
    id integer not null,
    name varchar(100) not null,
    parent_id integer
);
Alter Table GoodsHierarchy Add Constraint GoodsHierarchy_pk Primary Key (id);
Alter Table GoodsHierarchy Add Constraint GoodsHierarchy_GoodsHierarchy_fk Foreign Key (parent_id) References GoodsHierarchy(id);
