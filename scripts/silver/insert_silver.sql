/*
=============================================================
Stored Procedure: silver.load_silver
=============================================================
Script Purpose:
    This stored procedure loads and transforms data from the
    bronze layer into the silver layer tables.
    Each table is truncated before loading (Truncate & Insert pattern).

Transformations Applied:
    - Data type casting using TRY_CAST and CAST
    - TRIM applied to all string columns to remove whitespace
    - LOWER applied to categorical columns for standardization
    - HTML entity '&#039;' replaced with apostrophe in player names
    - location values standardized: 'H' → 'home', 'A' → 'away'
    - result values standardized: 'W' → 'win', 'L' → 'loss', 'D' → 'draw'
    - FLAG column added to src_appearances to track xGoalsChain anomalies

Tables Loaded:
    - silver.src_appearances
    - silver.src_games
    - silver.src_leagues
    - silver.src_players
    - silver.src_shots
    - silver.src_teams
    - silver.src_teamstats

Parameters:
    None

Usage:
    EXEC silver.load_silver;
=============================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME,@end_time DATETIME,@batch_st DATETIME,@batch_et DATETIME ;
		PRINT '==================================';
		PRINT 'Loading SILVER Layer';
		PRINT '==================================';
		SET @batch_st = GETDATE() ;
		SET @start_time = GETDATE() ;
		PRINT '==============================='
		PRINT 'Truncating table appearances'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_appearances;
		PRINT '======================================'
		PRINT 'inserting into silver.src_appearances'
		PRINT '======================================'
		INSERT INTO silver.src_appearances
		SELECT
			TRY_CAST(TRIM(gameID) AS INT) AS gameID,
			TRY_CAST(TRIM(playerID) AS INT) AS playerID,
			TRY_CAST(TRIM(goals) AS INT) AS goals,
			TRY_CAST(TRIM(owngoals) AS INT) AS owngoals,
			TRY_CAST(TRIM(shots) AS INT) AS shots,
			TRY_CAST(TRIM(xGoals) AS FLOAT) AS xGoals,
			TRY_CAST(TRIM(xGoalsChain) AS FLOAT) AS xGoalsChain,
			CASE 
				WHEN TRY_CAST(TRIM(xGoals) AS FLOAT) > TRY_CAST(TRIM(xGoalsChain) AS FLOAT)
				THEN 'xGoalsChain < xGoals'
				ELSE 'OK'
			END AS FLAG,
			TRY_CAST(TRIM(xGoalsBuildup) AS FLOAT) AS xGoalsBuildup ,
			TRY_CAST(TRIM(assists) AS INT) AS assists,
			TRY_CAST(TRIM(keyPasses) AS INT) AS keyPasses,
			TRY_CAST(TRIM(xAssists) AS FLOAT) AS xAssists,
			LOWER(TRIM(position)) AS position,
			TRY_CAST(TRIM(positionOrder) AS INT) AS positionOrder,
			TRY_CAST(TRIM(yellowCard) AS INT) AS yellowCard,
			TRY_CAST(TRIM(redCard) AS INT) AS redCard,
			TRY_CAST(TRIM(time) AS INT) AS time,
			TRY_CAST(TRIM(substituteIn) AS INT) AS substituteIn,
			TRY_CAST(TRIM(substituteOut) AS INT) AS substituteOut,
			TRY_CAST(TRIM(leagueID) AS INT) AS leagueID
		FROM bronze.src_appearances ;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		print '*******************************'

		SET @start_time = GETDATE() ;
		PRINT '==============================='
		PRINT 'Truncating table games'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_games;
		PRINT '==============================='
		PRINT 'inserting into silver.src_games'
		PRINT '==============================='
		INSERT INTO silver.src_games
		SELECT
			CAST(TRIM(gameID) AS INT) AS gameID,
			CAST(TRIM(leagueID) AS INT) AS leagueID,
			CAST(TRIM(season) AS INT) AS season,
			CAST(date AS DATETIME) AS date,
			CAST(homeTeamID AS INT) AS homeTeamID,
			CAST(awayTeamID AS INT) AS awayTeamID,
			CAST(homeGoals AS INT) AS homeGoals,
			CAST(awayGoals AS INT) AS awayGoals,
			CAST(homeProbability AS FLOAT) AS homeProbability,
			CAST(drawProbability AS FLOAT) AS drawProbability,
			CAST(awayProbability AS FLOAT) AS awayProbability,
			CAST(homeGoalsHalfTime AS INT) AS homeGoalsHalfTime,
			CAST(awayGoalsHalfTime AS INT) AS awayGoalsHalfTime
		FROM bronze.src_games;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		print '*******************************'

		SET @start_time = GETDATE() ;
		PRINT '==============================='
		PRINT 'Truncating table leagues'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_leagues;
		PRINT '=================================='
		PRINT 'inserting into silver.src_leagues'
		PRINT '=================================='
		INSERT INTO silver.src_leagues
		SELECT
			TRY_CAST(TRIM(leagueID) AS INT) leagueID,
			LOWER(TRIM(name)) name,
			LOWER(TRIM(understatNotation)) understatNotation
		FROM bronze.src_leagues;

		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		print '*******************************'

		SET @start_time = GETDATE() ;
		PRINT '==============================='
		PRINT 'Truncating table players'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_players;
		PRINT '=================================='
		PRINT 'inserting into silver.src_players'
		PRINT '=================================='
		INSERT INTO silver.src_players
		SELECT
			TRY_CAST(TRIM(playerID) AS INT) playerID,
			LOWER(REPLACE(TRIM(name), '&#039;', '''')) AS name
		FROM bronze.src_players;

		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		print '*******************************'

		SET @start_time = GETDATE() ;
		PRINT '==============================='
		PRINT 'Truncating table shots'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_shots;
		PRINT '==============================='
		PRINT 'inserting into silver.src_shots'
		PRINT '==============================='
		INSERT INTO silver.src_shots
		SELECT
			TRY_CAST(gameID AS INT) AS gameID,
			TRY_CAST(shooterID AS INT) AS shooterID,
			TRY_CAST(assisterID AS INT) AS assisterID,
			TRY_CAST(minute AS INT) AS minute,
			LOWER(TRIM(situation)) AS situation,
			LOWER(TRIM(lastAction)) AS lastAction,
			LOWER(TRIM(shotType)) AS shotType,
			LOWER(TRIM(shotResult)) AS shotResult,
			TRY_CAST(xGoal AS FLOAT) AS xGoal,
			TRY_CAST(positionX AS FLOAT) AS positionX,
			TRY_CAST(positionY AS FLOAT) AS positionY
		FROM bronze.src_shots;

		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'
		print '*******************************'

		SET @start_time = GETDATE() ;
		PRINT '==============================='
		PRINT 'Truncating table teams'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_teams;
		PRINT '================================'
		PRINT 'inserting into silver.src_teams'
		PRINT '================================'
		INSERT INTO silver.src_teams
		SELECT
			TRY_CAST(TRIM(teamID) AS INT) AS teamID,
			LOWER(TRIM(name)) AS name
		FROM bronze.src_teams;
		SET @end_time =GETDATE() ;
		PRINT '-->LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------'

		print '*******************************'

		SET @start_time = GETDATE() ;

		PRINT '==============================='
		PRINT 'Truncating table teamstats'
		PRINT '==============================='
		TRUNCATE TABLE silver.src_teamstats;
		PRINT '====================================='
		PRINT 'inserting into silver.src_teamstats'
		PRINT '====================================='
		INSERT INTO silver.src_teamstats
		SELECT
			TRY_CAST(TRIM(gameID) AS INT) AS gameID,
			TRY_CAST(teamID AS INT) AS teamID,
			TRY_CAST(season AS INT) AS season,
			CAST(date AS DATETIME) AS date,
			CASE
				WHEN UPPER(TRIM(location)) = 'H' THEN 'home'
				WHEN UPPER(TRIM(location)) = 'A' THEN 'away'
				ELSE 'NA'
			END location,
			TRY_CAST(goals AS INT) AS goals,
			TRY_CAST(xGoals AS FLOAT) AS xGoals,
			TRY_CAST(shots AS INT) AS shots,
			TRY_CAST(shotsOnTarget AS INT) AS shotsOnTarget,
			TRY_CAST(deep AS INT) AS deep,
			TRY_CAST(ppda AS FLOAT) AS ppda,
			TRY_CAST(fouls AS INT) AS fouls,
			TRY_CAST(corners AS INT) AS corners,
			TRY_CAST(yellowCards AS INT) AS yellowCards,
			TRY_CAST(redCards AS INT) AS redCards,
			CASE
				WHEN UPPER(TRIM(result)) = 'W' THEN 'win'
				WHEN UPPER(TRIM(result)) = 'L' THEN 'loss'
				WHEN UPPER(TRIM(result)) = 'D' THEN 'draw'
				ELSE 'NA'
			END result
		FROM bronze.src_teamstats;
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
		PRINT 'ERROR OCCURED DURING LOAD OF SILVER LAYER'
		PRINT 'ERROR MSG :' + ERROR_MESSAGE();
		PRINT '========================================='
		END CATCH
END
GO
EXEC silver.load_silver;



