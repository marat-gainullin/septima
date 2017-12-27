Create Table Customer (
    numeric id not null,
    varchar(200) name,
)
Alter Table Customer Add Constraint Customer_pk Primary Key (id)

Create Table Good (
    numeric id not null,
    varchar(200) name,
)
Alter Table Good Add Constraint Good_pk Primary Key (id)

Create Table GoodOrder (
    numeric id not null,
    numeric customer_id not null,
    numeric good_id not null,
    varchar(200) comment,
)
Alter Table GoodOrder Add Constraint GoodOrder_pk Primary Key (id)
Alter Table GoodOrder Add Constraint GoodOrder_Customer_fk Foreign Key References Customer(id)
Alter Table GoodOrder Add Constraint GoodOrder_God_fk Foreign Key References Good(id)
