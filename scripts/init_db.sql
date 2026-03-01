/*
=============================================================
Initialize Database: football_dwh
=============================================================
Script Purpose:
    This script initializes the Football Data Warehouse database.
    It drops the existing 'football_dwh' database if it exists,
    recreates it, and sets up the three layer schemas:
    'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will permanently delete the 'football_dwh'
    database and all its data if it exists.
    Ensure you have proper backups before running this script.
=============================================================
*/

use master;
-- Drop and recreate the database
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'football_dwh')
BEGIN
    ALTER DATABASE football_dwh SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE football_dwh;
END
GO
--create the database
create database football_dwh;
go
use football_dwh;
go
--create the schemas
create schema bronze;
go
create schema silver;
go
create schema gold;