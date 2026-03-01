/*
=============================================================
Stored Procedure: bronze.load_bronze
=============================================================
Script Purpose:
    This stored procedure loads raw data from CSV files into
    the bronze layer tables using BULK INSERT.
    Each table is truncated before loading to ensure a full
    refresh of the data (Truncate & Insert pattern).

Tables Loaded:
    - bronze.src_appearances
    - bronze.src_games
    - bronze.src_leagues
    - bronze.src_players
    - bronze.src_shots
    - bronze.src_teams
    - bronze.src_teamstats

Parameters:
    None

Usage:
    EXEC bronze.load_bronze;

WARNING:
    - Update the file paths in each BULK INSERT statement
      to match your local dataset directory before running.
    - Running this procedure will truncate all bronze tables
      and reload them from the CSV source files.
=============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME,@end_time DATETIME,@batch_st DATETIME,@batch_et DATETIME ;
		PRINT '==================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==================================';

		SET @batch_st = GETDATE() ;
		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_appearances';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_appearances';
		TRUNCATE TABLE bronze.src_appearances ;
		PRINT 'INSERTING DATA INTO : bronze.src_appearances';
		BULK INSERT bronze.src_appearances
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\appearances.csv'
		WITH ( 
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_games';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_games';
		TRUNCATE TABLE bronze.src_games ;
		PRINT 'INSERTING DATA INTO : bronze.src_games';
		BULK INSERT bronze.src_games
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\games.csv'
		WITH ( 
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_leagues';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_leagues';
		TRUNCATE TABLE bronze.src_leagues ;
		PRINT 'INSERTING DATA INTO : bronze.src_leagues';
		BULK INSERT bronze.src_leagues
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\leagues.csv'
		WITH ( 
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_players';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_players';
		TRUNCATE TABLE bronze.src_players ;
		PRINT 'INSERTING DATA INTO : bronze.src_players';
		BULK INSERT bronze.src_players
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\players.csv'
		WITH ( 
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			CODEPAGE = '1252',  
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_shots';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_shots';
		TRUNCATE TABLE bronze.src_shots ;
		PRINT 'INSERTING DATA INTO : bronze.src_shots';
		BULK INSERT bronze.src_shots
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\shots.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_teams';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_teams';
		TRUNCATE TABLE bronze.src_teams ;
		PRINT 'INSERTING DATA INTO : bronze.src_teams';
		BULK INSERT bronze.src_teams
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\teams.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE() ;
		PRINT '==================================';
		PRINT 'Loading bronze.src_teamstats';
		PRINT '==================================';
		PRINT '--> TRUNCATING TABLE : bronze.src_teamstats';
		TRUNCATE TABLE bronze.src_teamstats ;
		PRINT 'INSERTING DATA INTO : bronze.src_teamstats';
		BULK INSERT bronze.src_teamstats
		FROM 'D:\SQL\sql_football_dataset_dwh\datasets\teamstats.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		) ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		SET @batch_et =GETDATE() ;
		PRINT '------------------'
		PRINT '-->BATCH DURATION : ' + CAST(DATEDIFF(second,@batch_st,@batch_et) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'
		END TRY

		BEGIN CATCH
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOAD OF BRONZE LAYER'
		PRINT 'ERROR MSG :' + ERROR_MESSAGE();
		PRINT '========================================='
		END CATCH
END
GO
EXEC bronze.load_bronze;



