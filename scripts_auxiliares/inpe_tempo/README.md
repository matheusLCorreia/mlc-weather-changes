docker run --name weather_db \
-e POSTGRES_PASSWORD=1234 \
-e PGDATA=/var/lib/postgresql/data/pgdata \
-v /var/postgres/data:/var/lib/postgresql/data \
-p 5432:5432
-d postgres;

## DATABASE
create database weather_db;

## TABLES
create table cities (id integer, nome varchar(200), uf varchar(2));