/*
=============================================================
Database Creation and Schema Setup
=============================================================
Script Objective:
    This script is designed to create a new database named 'DataWarehouse' after verifying its existence.
    If the database is already present, it will be dropped and recreated. Additionally, three schemas 
    ('bronze', 'silver', and 'gold') will be established within the database.

CAUTION:
    Executing this script will remove the 'DataWarehouse' database if it exists. 
    All stored data will be permanently lost. Proceed with caution and ensure 
    that appropriate backups are available before execution.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
