Create Table public.assets (
    id decimal(22, 0) not null,
    name varchar(400) not null,
    field3 varchar(100),
    field4 varchar(100),
    field5 decimal(22, 0),
    field6 decimal(22, 0),
    field7 double
);
Alter Table public.assets Add Constraint public.assets_pk Primary Key(id);
Create Index public.i127185895762216 On public.assets(field6);

Create Table public.asset_groups (
    id decimal(38, 19) not null,
    name varchar(100) not null,
    p_id decimal(38, 19)
);
Alter Table public.asset_groups Add Constraint public.asset_groups_pk Primary Key(ID);
Alter Table public.asset_groups Add Constraint public.fk_asset_groups_parent Foreign Key(p_id) References public.asset_groups(id) On Delete Cascade;

Create Table public.asset_kinds (
    id decimal(38, 19) not null,
    name varchar(100) not null,
    field1 varchar(100),
    field2 decimal(38, 0),
    field3 timestamp,
    field4 blob(4294967295),
    field5 decimal(38, 0)
);
Alter Table public.asset_kinds Add Constraint public.asset_kinds_pk Primary Key(id);

Create Table public.asset_types (
    id decimal(38, 19) not null,
    name varchar(100),
    datedata timestamp
);
Alter Table public.asset_types Add Constraint public.asset_types_pk Primary Key(id);

Create Table public.customer (
    customer_id decimal(38, 19) not null,
    customer_name varchar(100),
    customer_address varchar(1000),
    parent decimal(38, 19)
);
Alter Table public.customer Add Constraint public.customer_pk Primary Key(customer_id);

Create Table public.GOOD (
    good_id decimal(38, 19) not null,
    good_name varchar(100)
);
Alter Table public.good Add Constraint public.good_pk Primary Key(good_id);

Create Table public.goodorder (
    order_id decimal(38, 19) not null Comment 'Order key',
    amount decimal(38, 19) Comment 'Goods amount',
    good decimal(38, 19) Comment 'Ordered good',
    customer decimal(38, 19) Comment 'Good orderer',
    field1 blob(4294967295)
);
Alter Table public.goodorder Add Constraint public.goodorder_pk Primary Key(order_id);
Alter Table public.goodorder Add Constraint public.fk_131158275681283 Foreign Key(customer) References public.customer(customer_id) On Delete Cascade;
Alter Table public.goodorder Add Constraint public.fk_141171593226029 Foreign Key(good) References public.good(good_id) On Delete Cascade;

Create Table public.delaware_administrative(
    delaware_administrative_id decimal(38, 19) not null,
    the_geom geometry,
    name varchar(19),
    admin_leve varchar(1)
);
Alter Table public.delaware_administrative Add Constraint public.delaware_administrative_pk Primary Key(delaware_administrative_id);

Create Table public.mtd_entities (
    mdent_name varchar(150),
    mdent_type decimal(38, 19) not null,
    mdent_content_txt clob(4294967295),
    mdent_content_data blob(4294967295),
    tag1 varchar(100),
    tag2 varchar(100),
    tag3 varchar(100),
    mdent_order double,
    mdent_content_txt_size decimal(38, 19),
    mdent_content_txt_crc32 decimal(38, 19),
    mdent_id varchar(200) not null,
    mdent_parent_id varchar(200)
);
Alter Table public.mtd_entities Add Constraint public.mtd_entities_pk Primary Key(mdent_id);
Create Unique Index public.mtd_entities_uk221336657663781 on public.mtd_entities(mdent_parent_id, mdent_type, mdent_name);

Create Table public.mtd_mdchnglog (
    mdlog_id decimal(38, 19) not null,
    opdate timestamp,
    opsession_user varchar(100) not null,
    ophost varchar(100) not null,
    opterminal varchar(100) not null,
    optype varchar(100) not null,
    opobjtype varchar(100) not null,
    opobjname varchar(100) not null,
    opos_user varchar(100) not null,
    op_client_ip_address varchar(100),
    op_ddl varchar(2048)
);
Alter Table public.mtd_mdchnglog Add Constraint public.mtd_mdchnglog_pk Primary Key(mdlog_id);

Create Table public.table1(
    id decimal(38, 19) not null,
    f1 decimal(38, 19),
    f2 decimal(38, 19),
    f3 decimal(38, 19)
);
Alter Table public.table1 Add Constraint public.table1_pk Primary Key(id);

Create Table public.TABLE2(
    id decimal(38, 19) not null,
    fielda decimal(38, 19),
    fieldb decimal(38, 19),
    fieldc decimal(38, 19)
);
Alter Table public.table2 Add Constraint public.table2_pk Primary Key(id);

Create Alias public.calculator As $$
    long septimaSampleCalc(int first, long second){
        return first + second;
    }
$$;

Insert Into public.assets(id, name, field3, field4, field5, field6, field7) Values
(128015357440672, 'building1', 'rt45', 'fg67', 6, -11, null),
(128030527792115, 'building2', 'gr43', 'nh89', 5, 3, null);

Insert Into public.asset_groups(id, name, p_id) Values
(1.0000, 'Производственные ОС', null),
(2.0000, 'Непроизводственные ОС', null),
(3.0000, 'Станки деревообрабатывающие', 1.0000),
(4.0000, 'Прочие производственные ОС', 1.0000),
(5.0000, 'Промышленно-производственные ОС', 1.0000);

Insert Into public.asset_kinds(id, name, field1, field2, field3, field4, field5) Values
(1.0000, 'Основное оборудование', null, null, null, null, null),
(2.0000, 'Вспомогательное оборудование', null, null, null, null, null),
(3.0000, 'Вычислительная техника', null, null, null, null, null);

Insert Into public.asset_types(id, name, datedata) Values
(1.0000, 'Механическое оборудование', null),
(2.0000, 'Электрическое оборудование', null),
(3.0000, 'Здания и сооружения', null),
(4.0000, 'Средства измерения', null),
(5.0000, 'Транспортные средства', null),
(6.0000, 'Вычислительная техника', null);

Insert Into public.customer(customer_id, customer_name, customer_address, parent) Values
(1.0000, 'Sun Microsystems', ' 2211dsfasdfss', null),
(2.0000, 'IBM', null, null),
(3.0000, 'Microsoft', 'г. Кракозябры, ул Кырозубры, д. 6', null),
(4.0000, 'Microsift', 'asdaas22', 3.0000),
(6.0000, 'UBM', 'didr', 2.0000),
(7.0000, 'ABM 22', 'didr', 2.0000),
(8.0000, 'Solaris', 'adr', 1.0000),
(9.0000, 'Gelios', 'qweq 22', 1.0000);

Insert Into public.delaware_administrative(delaware_administrative_id, the_geom, name, admin_leve) Values
(2.0000, 'MULTILINESTRING ((-75.784802 39.819017, -75.786246 39.818865, -75.787034 39.8199309, -75.786044 39.829671, -75.786022 39.829678, -75.785328 39.82978, -75.784718 39.829869, -75.782725 39.830163, -75.781844 39.830292, -75.781806 39.830298, -75.781764 39.830304, -75.777907 39.83056, -75.777194 39.830607, -75.776204 39.830673, -75.776114 39.829656, -75.776144 39.829493, -75.776199 39.829214, -75.776208 39.829165, -75.775981 39.826719, -75.77571 39.825093, -75.7755619 39.823422, -75.775531 39.823071, -75.775314 39.820621, -75.775261 39.82002, -75.775296 39.820017, -75.780919 39.819425, -75.783436 39.819161, -75.783496 39.819155, -75.783561 39.819148, -75.783633 39.81914, -75.784802 39.819017))', null, '8');

Insert Into public.good(good_id, good_name) Values
(1.0000, 'Compact disks'),
(2.0000, 'Condoms'),
(3.0000, 'Tanks'),
(4.0000, 'Oil');

Insert Into public.goodorder(order_id, amount, good, customer, field1) Values
(1.0000, 300.0000, 1.0000, 1.0000, null),
(2.0000, 2345.0000, 2.0000, 1.0000, null),
(3.0000, 1231.0000, 1.0000, 2.0000, null);

Insert Into public.mtd_entities(mdent_name, mdent_type, mdent_content_txt, mdent_content_data, tag1, tag2, tag3, mdent_order, mdent_content_txt_size, mdent_content_txt_crc32, mdent_id, mdent_parent_id) Values
('legacy entity 1', 50, 'legacy entity 1 content', null, null, null, null, null, 1896, 4061411100, '124349292311931632', null),
('lagecy entity 2', 50, 'legacy entity 2 content', null, null, null, null, null, 1143, 3851086996, '124832514140608864', '124349292311931632'),
('lagecy entity 3', 70, 'legacy entity 3 content', null, null, null, null, null, 615, 869060358, '128032313214099', null),
('lagecy entity 4', 70, 'legacy entity 4 content', null, null, null, null, null, 592, 1655896106, '128049551290614', null),
('lagecy entity 5', 20, 'legacy entity 5 content', null, null, null, null, null, 593, 1655896106, '128049551290615', null),
('lagecy entity 6', 20, 'legacy entity 6 content', null, null, null, null, null, 594, 1655896106, '128049551290616', '124349292311931632'),
('lagecy entity 7', 20, 'legacy entity 7 content', null, null, null, null, null, 595, 1655896106, '128049551290617', '124349292311931632');

Insert Into public.table1(id, f1, f2, f3) Values
(1.0000, 111.0000, 112.0000, 113.0000),
(2.0000, 121.0000, 1530966857.0000, 1419752413.0000),
(3.0000, 131.0000, -409347514.0000, 133.0000),
(4.0000, 141.0000, 142.0000, 143.0000);

Insert Into public.table2(id, fielda, fieldb, fieldc) Values
(0.0000, 1.0000, 2.0000, 3.0000),
(100.0000, 101.0000, 102.0000, 103.0000),
(104.0000, 105.0000, 106.0000, 107.0000);
