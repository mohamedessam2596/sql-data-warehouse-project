/*
By this script we create Data warehouse including Three schemas Bronze,silver and gold

note before we create Data warehouse we check if it exisit if it we drop it and the data will be lost

*/



use master

go

IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'Datawarehouse'
)
BEGIN
    DROP DATABASE Datawarehouse;
END;

create database Datawarehouse;

go

use Datawarehouse

go

create schema bronze

go

create schema silver

go

create schema gold

go
