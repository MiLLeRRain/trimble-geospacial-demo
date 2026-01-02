
SELECT * FROM sys.database_principals

SELECT DB_NAME() AS CurrentDb, SUSER_SNAME() AS LoginName;


CREATE USER [trimble-geospatial-api] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [trimble-geospatial-api];
ALTER ROLE db_datawriter ADD MEMBER [trimble-geospatial-api];


CREATE USER [trimble-geospatial-demo-func] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [trimble-geospatial-demo-func];
ALTER ROLE db_datawriter ADD MEMBER [trimble-geospatial-demo-func];

