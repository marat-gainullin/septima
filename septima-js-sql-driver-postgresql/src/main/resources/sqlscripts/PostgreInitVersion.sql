create table app_version
(
    version_value numeric not null,
    constraint app_version_pk primary key(version_value)
)
#GO
insert into app_version (version_value) values (0)
#GO
