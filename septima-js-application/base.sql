drop all objects;

create user if not exists sa salt 'f6e293b0d7d6a619' hash '50007d9c9583418d29b0d79144556ca7ce646060aacc1af32ffbe2dbf9a4d55e' admin;
create cached table public.owners(
    owners_id decimal(18, 0) not null,
    firstName varchar(100),
    lastName varchar(100),
    address varchar(100),
    city varchar(100),
    telephone varchar(100),
    email varchar(100)
);
alter table public.owners add constraint public.owners_pk primary key(owners_id);
-- 2 +/- select count(*) from public.owners;
insert into public.owners(owners_id, firstName, lastName, address, city, telephone, email) values
(142841788496711, 'Ivan', 'Ivanov', 'Ivanovskaya st.', 'Ivanovo', '+79011111111', 'sample@example.com'),
(142841834950629, 'Petr', 'Petrov', 'Petrovskaya', 'Saint Petersburg', '+79022222222', 'test@test.ru');
create table public.pets(
    pets_id decimal(18, 0) not null,
    owner_id decimal(65535, 32767) not null,
    type_id decimal(65535, 32767) not null,
    name varchar(100),
    birthdate timestamp
);
alter table public.pets add constraint public.pets_pk primary key(pets_id);
-- 3 +/- SELECT COUNT(*) FROM PUBLIC.PETS;
insert into pets(pets_id, owner_id, type_id, name, birthDate) values
(142841880961396, 142841788496711, 142841300122653, 'Druzhok', NULL),
(142841883974964, 142841834950629, 142841300155478, 'Vasya', parsedatetime('2015-04-29 00:00:00.0', 'yyyy-MM-dd HH:mm:ss.SSS', 'en', 'GMT+3')),
(143059430815590, 142841788496711, 142850046716850, 'Tom', NULL),
(143059430815594, 142841788496711, 142850046716850, 'Pik', NULL);
CREATE TABLE petsSnapshot(
    pets_id decimal(18, 0) not null,
    owner_id decimal(65535, 32767) not null,
    type_id decimal(65535, 32767) not null,
    name varchar(100),
    birthDate timestamp
);
alter table petsSnapshot add constraint public.petsSnapshot_pk primary key(pets_id);
-- 3 +/- select count(*) from public.pets;
insert into petsSnapshot(pets_id, owner_id, type_id, name, birthdate) values
(142841880961396, 142841788496711, 142841300122653, 'Druzhok', null),
(142841883974964, 142841834950629, 142841300155478, 'Vasya', parsedatetime('2015-04-29 00:00:00.0', 'yyyy-MM-dd HH:mm:ss.SSS', 'en', 'GMT+3')),
(143059430815594, 142841788496711, 142850046716850, 'Pik', null);

create table petTypes(
    pettypes_id decimal(18, 0) not null,
    name varchar(100)
);
alter table public.petTypes add constraint petTypes_pk primary key(petTypes_id);
-- 3 +/- select count(*) from public.petTypes;
insert into public.petTypes(petTypes_id, name) values
(142841300122653, 'Dog'),
(142841300155478, 'Cat'),
(142850046716850, 'Mouse');
create cached table public.temp(
    temp_id decimal(18, 0) not null,
    field1 blob(100) not null
);
alter table public.temp add constraint constraint_2 primary key(temp_id);
-- 0 +/- select count(*) from public.temp;
create cached table public.dummyTable(
    dummy decimal(18, 0)
);
-- 0 +/- select count(*) from public.dummyTable;
create cached table visit(
    visit_id decimal(18, 0) not null,
    pet_id decimal(65535, 32767) not null,
    fromDate timestamp,
    toDate timestamp,
    description varchar(100),
    isPaid boolean
);
alter table public.visit add constraint public.visit_pk primary key(visit_id);
-- 3 +/- select count(*) from public.visit;
insert into public.visit(visit_id, pet_id, fromDate, toDate, description, isPaid) values
(143023673259940, 142841883974964, timestamp '2015-04-28 18:58:52.604', timestamp '2015-04-29 00:00:00.0', null, null),
(143031982989403, 142841880961396, timestamp '2015-04-29 18:03:49.898', null, null, null),
(143029901200462, 142841883974964, timestamp '2015-04-29 12:16:52.008', timestamp '2015-04-30 00:00:00.0', '1234', null);
alter table public.pets add constraint public.fk_143039780889568 foreign key(type_id) references public.petTypes(petTypes_id) on delete cascade on update cascade noCheck;
alter table public.pets add constraint public.fk_137568650945995 foreign key(owner_id) references public.owners(owners_id) on delete cascade on update cascade noCheck;
alter table public.visit add constraint public.fk_137568671360207 foreign key(pet_id) references public.pets(pets_id) on delete cascade on update cascade noCheck;