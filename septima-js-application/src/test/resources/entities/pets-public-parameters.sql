select * from pets p
where
:flag = true and
p.owner_id = :owner_id and
p.birthdate > :birthFrom and
p.name = :name
