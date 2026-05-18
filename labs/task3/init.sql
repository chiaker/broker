create table if not exists items (
    id int primary key,
    value text not null,
    updated_at timestamp not null default now()
);

insert into items (id, value)
select g, 'init-' || md5(g::text)
from generate_series(1, 1000) g
on conflict (id) do nothing;
