create schema if not exists raw_nn;

create table if not exists raw_nn.directory(
    updated_at timestamp default current_timestamp not null,
    v jsonb 
);

create table if not exists raw_nn.enrollment(
    updated_at timestamp default current_timestamp not null,
    v jsonb 
);

create table if not exists raw_nn.assessment(
    updated_at timestamp default current_timestamp not null,
    v jsonb 
);




