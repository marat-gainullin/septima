select k.id k_id, k.name k_name, t.id t_id, t.name t_name, g.id g_id, g.name g_name from asset_kinds k
inner join asset_types t on (k.id = t.id)
inner join asset_groups g on (k.id = g.id)