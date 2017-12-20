create table app_users
(
  usr_name character varying(200) not null,
  usr_passwd character varying(200),
  usr_form character varying(200),
  usr_context character varying(200),
  usr_roles character varying(200),
  usr_phone character varying(200),
  usr_email character varying(200),
  constraint app_users_pk primary key (usr_name)
)
#GO
create table app_users_groups
(
    usr_name varchar(200) not null,
    group_name varchar(200) not null
)
#GO
alter table app_users_groups
    add constraint app_users_groups_users_fk foreign key(usr_name) references app_users(usr_name) on delete cascade on update cascade
#GO
insert into app_users (usr_name, usr_passwd)
    values ('admin', 'abe6db4c9f5484fae8d79f2e868a673c')
#GO
insert into app_users_groups (usr_name, group_name)
    values ('admin', 'admin')
#GO