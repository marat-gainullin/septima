Create Table public.ASSETS (
    id decimal(22, 0) not null,
    name varchar(400) not null,
    field3 varchar(100),
    field4 varchar(100),
    field5 decimal(22, 0),
    field6 decimal(22, 0),
    field7 double
);
Alter Table public.ASSETS Add Constraint public.ASSETS_PK Primary Key(ID);
Create Index public.I127185895762216 ON PUBLIC.ASSETS(FIELD6);

Create Table public.ASSET_GROUPS (
    id decimal(38, 19) not null,
    name varchar(100) not null,
    p_id decimal(38, 19)
);
Alter Table public.ASSET_GROUPS Add Constraint public.ASSET_GROUPS_PK Primary Key(ID);
Alter Table public.ASSET_GROUPS Add Constraint public.FK_ASSET_GROUPS_PARENT FOREIGN KEY(P_ID) REFERENCES PUBLIC.ASSET_GROUPS(ID) ON DELETE CASCADE NOCHECK;

Create Table public.ASSET_KINDS (
    id decimal(38, 19) not null,
    name varchar(100) not null,
    field1 varchar(100),
    field2 decimal(38, 0),
    field3 timestamp,
    field4 blob(4294967295),
    field5 decimal(38, 0)
);
Alter Table public.ASSET_KINDS Add Constraint public.ASSET_KINDS_PK Primary Key(ID);

Create Table public.ASSET_TYPES (
    id decimal(38, 19) not null,
    name varchar(100),
    datedata timestamp
);
Alter Table public.ASSET_TYPES Add Constraint public.ASSET_TYPES_PK Primary Key(ID);

Create Table public.CUSTOMER (
    customer_id decimal(38, 19) not null,
    customer_name varchar(100),
    customer_address varchar(1000),
    parent decimal(38, 19)
);
Alter Table public.CUSTOMER Add Constraint public.CUSTOMER_PK Primary Key(CUSTOMER_ID);

Create Table public.GOOD (
    good_id decimal(38, 19) not null,
    good_name varchar(100)
);
Alter Table public.GOOD Add Constraint public.GOOD_PK Primary Key(GOOD_ID);

Create Table public.GOODORDER (
    order_id decimal(38, 19) not null Comment 'Order key',
    amount decimal(38, 19) Comment 'Goods amount',
    good decimal(38, 19) Comment 'Ordered good',
    customer decimal(38, 19) Comment 'Good orderer',
    field1 blob(4294967295)
);
Alter Table public.GOODORDER Add Constraint public.GOODORDER_PK Primary Key(ORDER_ID);
Alter Table public.GOODORDER Add Constraint public.FK_131158275681283 FOREIGN KEY(CUSTOMER) REFERENCES PUBLIC.CUSTOMER(CUSTOMER_ID) ON DELETE CASCADE NOCHECK;
Alter Table public.GOODORDER Add Constraint public.FK_141171593226029 FOREIGN KEY(GOOD) REFERENCES PUBLIC.GOOD(GOOD_ID) ON DELETE CASCADE NOCHECK;

Create Table public.DELAWARE_ADMINISTRATIVE(
    delaware_administrative_id decimal(38, 19) not null,
    the_geom geometry,
    name varchar(19),
    admin_leve varchar(1)
);
Alter Table public.DELAWARE_ADMINISTRATIVE Add Constraint public.DELAWARE_ADMINISTRATIVE_PK Primary Key(DELAWARE_ADMINISTRATIVE_ID);

Create Table public.MTD_ENTITIES (
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
Alter Table public.MTD_ENTITIES Add Constraint public.MTD_ENTITIES_PK Primary Key(MDENT_ID);
CREATE UNIQUE INDEX PUBLIC.MTD_ENTITIES_UK221336657663781 ON PUBLIC.MTD_ENTITIES(MDENT_PARENT_ID, MDENT_TYPE, MDENT_NAME);

Create Table public.MTD_MDCHNGLOG (
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
Alter Table public.MTD_MDCHNGLOG Add Constraint public.MTD_MDCHNGLOG_PK Primary Key(MDLOG_ID);

Create Table public.TABLE1(
    id decimal(38, 19) not null,
    f1 decimal(38, 19),
    f2 decimal(38, 19),
    f3 decimal(38, 19)
);
Alter Table public.TABLE1 Add Constraint public.TABLE1_PK Primary Key(ID);

Create Table public.TABLE2(
    id decimal(38, 19) not null,
    fielda decimal(38, 19),
    fieldb decimal(38, 19),
    fieldc decimal(38, 19)
);
Alter Table public.TABLE2 Add Constraint public.TABLE2_PK Primary Key(ID);

Create Alias public.calculator As $$
    long septimaSampleCalc(int first, long second){
        return first + second;
    }
$$;

Insert Into public.assets(id, name, field3, field4, field5, field6, field7) values
(128015357440672, 'building1', 'rt45', 'fg67', 6, -11, Null),
(128030527792115, 'building2', 'gr43', 'nh89', 5, 3, Null);

Insert Into public.asset_groups(id, name, p_id) values
(1.0000, STRINGDECODE('\u041f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0435 \u041e\u0421'), Null),
(2.0000, STRINGDECODE('\u041d\u0435\u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0435 \u041e\u0421'), Null),
(3.0000, STRINGDECODE('\u0421\u0442\u0430\u043d\u043a\u0438 \u0434\u0435\u0440\u0435\u0432\u043e\u043e\u0431\u0440\u0430\u0431\u0430\u0442\u044b\u0432\u0430\u044e\u0449\u0438\u0435'), 1.0000),
(4.0000, STRINGDECODE('\u041f\u0440\u043e\u0447\u0438\u0435 \u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0435 \u041e\u0421'), 1.0000),
(5.0000, STRINGDECODE('\u041f\u0440\u043e\u043c\u044b\u0448\u043b\u0435\u043d\u043d\u043e-\u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0435 \u041e\u0421'), 1.0000);

Insert Into public.asset_kinds(id, name, field1, field2, field3, field4, field5) values
(1.0000, STRINGDECODE('\u041e\u0441\u043d\u043e\u0432\u043d\u043e\u0435 \u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435'), Null, Null, Null, Null, Null),
(2.0000, STRINGDECODE('\u0412\u0441\u043f\u043e\u043c\u043e\u0433\u0430\u0442\u0435\u043b\u044c\u043d\u043e\u0435 \u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435'), Null, Null, Null, Null, Null),
(3.0000, STRINGDECODE('\u0412\u044b\u0447\u0438\u0441\u043b\u0438\u0442\u0435\u043b\u044c\u043d\u0430\u044f \u0442\u0435\u0445\u043d\u0438\u043a\u0430'), Null, Null, Null, Null, Null);

Insert Into public.asset_types(id, name, datedata) values
(1.0000, STRINGDECODE('\u041c\u0435\u0445\u0430\u043d\u0438\u0447\u0435\u0441\u043a\u043e\u043e\u0435 \u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435'), Null),
(2.0000, STRINGDECODE('\u042d\u043b\u0435\u043a\u0442\u0440\u0438\u0447\u0435\u0441\u043a\u043e\u0435 \u043e\u0431\u043e\u0440\u0443\u0434\u043e\u0432\u0430\u043d\u0438\u0435'), Null),
(3.0000, STRINGDECODE('\u0417\u0434\u0430\u043d\u0438\u044f \u0438 \u0441\u043e\u043e\u0440\u0443\u0436\u0435\u043d\u0438\u044f'), Null),
(4.0000, STRINGDECODE('\u0421\u0440\u0435\u0434\u0441\u0442\u0432\u0430 \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u044f'), Null),
(5.0000, STRINGDECODE('\u0422\u0440\u0430\u043d\u0441\u043f\u043e\u0440\u0442\u043d\u044b\u0435 \u0441\u0440\u0435\u0434\u0441\u0442\u0432\u0430'), Null),
(6.0000, STRINGDECODE('\u0412\u044b\u0447\u0438\u0441\u043b\u0438\u0442\u0435\u043b\u044c\u043d\u0430\u044f \u0442\u0435\u0445\u043d\u0438\u043a\u0430'), Null);

Insert Into public.customer(customer_id, customer_name, customer_address, parent) values
(1.0000, 'Sun Microsystems', ' 2211dsfasdfss', Null),
(2.0000, 'IBM', Null, Null),
(3.0000, 'Microsoft', STRINGDECODE('\u0433. \u041a\u0440\u0430\u043a\u043e\u0437\u044f\u0431\u0440\u044b, \u0443\u043b \u041a\u044b\u0440\u043e\u0437\u0443\u0431\u0440\u044b, \u0434. 140500221699981'), Null),
(4.0000, 'Microsift', 'asdaas22', 3.0000),
(6.0000, 'UBM', 'didr', 2.0000),
(7.0000, 'ABM 22', 'didr', 2.0000),
(8.0000, 'Solaris', 'adr', 1.0000),
(9.0000, 'Gelios', 'qweq 22', 1.0000);

Insert Into public.delaware_administrative(delaware_administrative_id, the_geom, name, admin_leve) values
(2.0000, 'MULTILINESTRING ((-75.784802 39.819017, -75.786246 39.818865, -75.787034 39.8199309, -75.786044 39.829671, -75.786022 39.829678, -75.785328 39.82978, -75.784718 39.829869, -75.782725 39.830163, -75.781844 39.830292, -75.781806 39.830298, -75.781764 39.830304, -75.777907 39.83056, -75.777194 39.830607, -75.776204 39.830673, -75.776114 39.829656, -75.776144 39.829493, -75.776199 39.829214, -75.776208 39.829165, -75.775981 39.826719, -75.77571 39.825093, -75.7755619 39.823422, -75.775531 39.823071, -75.775314 39.820621, -75.775261 39.82002, -75.775296 39.820017, -75.780919 39.819425, -75.783436 39.819161, -75.783496 39.819155, -75.783561 39.819148, -75.783633 39.81914, -75.784802 39.819017))', Null, '8');

Insert Into public.good(good_id, good_name) values
(1.0000, 'Compact disks'),
(2.0000, 'Condoms'),
(3.0000, 'Tanks'),
(4.0000, 'Oil');

Insert Into public.goodorder(order_id, amount, good, customer, field1) values
(1.0000, 300.0000, 1.0000, 1.0000, Null),
(2.0000, 2345.0000, 2.0000, 1.0000, Null),
(3.0000, 1231.0000, 1.0000, 2.0000, Null);

Insert Into public.mtd_entities(mdent_name, mdent_type, mdent_content_txt, mdent_content_data, tag1, tag2, tag3, mdent_order, mdent_content_txt_size, mdent_content_txt_crc32, mdent_id, mdent_parent_id) values
('legacy entity 1', 50, 'legacy entity 1 content', Null, Null, Null, Null, Null, 1896, 4061411100, '124349292311931632', Null),
('lagecy entity 2', 50, 'legacy entity 2 content', Null, Null, Null, Null, Null, 1143, 3851086996, '124832514140608864', '124349292311931632'),
('lagecy entity 3', 70, 'legacy entity 3 content', Null, Null, Null, Null, Null, 615, 869060358, '128032313214099', Null),
('lagecy entity 4', 70, 'legacy entity 4 content', Null, Null, Null, Null, Null, 592, 1655896106, '128049551290614', Null),
('lagecy entity 5', 20, 'legacy entity 5 content', Null, Null, Null, Null, Null, 593, 1655896106, '128049551290615', Null),
('lagecy entity 6', 20, 'legacy entity 6 content', Null, Null, Null, Null, Null, 594, 1655896106, '128049551290616', '124349292311931632'),
('lagecy entity 7', 20, 'legacy entity 7 content', Null, Null, Null, Null, Null, 595, 1655896106, '128049551290617', '124349292311931632');

Insert Into public.table1(id, f1, f2, f3) values
(1.0000, 111.0000, 112.0000, 113.0000),
(2.0000, 121.0000, 1530966857.0000, 1419752413.0000),
(3.0000, 131.0000, -409347514.0000, 133.0000),
(4.0000, 141.0000, 142.0000, 143.0000);

Insert Into public.table2(id, fielda, fieldb, fieldc) values
(0.0000, 1.0000, 2.0000, 3.0000),
(100.0000, 101.0000, 102.0000, 103.0000),
(104.0000, 105.0000, 106.0000, 107.0000);

