/*
=============================================================
DDL: Bronze Layer Tables
=============================================================
Script Purpose:
    This script creates all raw staging tables in the 'bronze' schema.
    All columns are defined as NVARCHAR to preserve raw data exactly
    as received from the source CSV files without any transformation.
    If a table already exists, it is dropped and recreated.

Tables Created:
    - bronze.src_appearances
    - bronze.src_games
    - bronze.src_leagues
    - bronze.src_players
    - bronze.src_shots
    - bronze.src_teams
    - bronze.src_teamstats

WARNING:
    Running this script will drop and recreate all bronze tables.
    All existing data in the bronze layer will be permanently deleted.
=============================================================
*/

PRINT '============================='
PRINT 'CREATING BRONZE LAYER TABLES'
PRINT '============================='

IF OBJECT_ID('bronze.src_appearances','U') IS NOT NULL
	DROP TABLE bronze.src_appearances ;
CREATE TABLE bronze.src_appearances(
	gameID NVARCHAR(50),
	playerID NVARCHAR(50),
	goals NVARCHAR(50),
	ownGoals NVARCHAR(50),
	shots NVARCHAR(50),
	xGoals NVARCHAR(50),
	xGoalsChain NVARCHAR(50),
	xGoalsBuildup NVARCHAR(50),
	assists NVARCHAR(50),
	keyPasses NVARCHAR(50),
	xAssists NVARCHAR(50),
	position NVARCHAR(50),
	positionOrder NVARCHAR(50),
	yellowCard NVARCHAR(50),
	redCard NVARCHAR(50),
	time NVARCHAR(50),
	substituteIn NVARCHAR(50),
	substituteOut NVARCHAR(50),
	leagueID NVARCHAR(50)
) ;

IF OBJECT_ID('bronze.src_games','U') IS NOT NULL
	DROP TABLE bronze.src_games ;
CREATE TABLE bronze.src_games(
	gameID	NVARCHAR(50),
	leagueID NVARCHAR(50),
	season NVARCHAR(50),
	date NVARCHAR(100),
	homeTeamID NVARCHAR(50),
	awayTeamID NVARCHAR(50),
	homeGoals NVARCHAR(50),
	awayGoals NVARCHAR(50),
	homeProbability NVARCHAR(50),
	drawProbability NVARCHAR(50),
	awayProbability NVARCHAR(50),
	homeGoalsHalfTime NVARCHAR(50),
	awayGoalsHalfTime NVARCHAR(50)
) ;

IF OBJECT_ID('bronze.src_leagues','U') IS NOT NULL
	DROP TABLE bronze.src_leagues ;
CREATE TABLE bronze.src_leagues(
	leagueID NVARCHAR(50),
	name NVARCHAR(50),
	understatNotation NVARCHAR(50)
);

IF OBJECT_ID('bronze.src_players','U') IS NOT NULL
	DROP TABLE bronze.src_players ;
CREATE TABLE bronze.src_players(
	playerID NVARCHAR(50),
	name NVARCHAR(100)
) ;


IF OBJECT_ID('bronze.src_shots','U') IS NOT NULL
	DROP TABLE bronze.src_shots ;
CREATE TABLE bronze.src_shots(
	gameID NVARCHAR(50),
	shooterID NVARCHAR(50),
	assisterID NVARCHAR(50),
	minute NVARCHAR(50),
	situation NVARCHAR(50),
	lastAction NVARCHAR(50),
	shotType NVARCHAR(50),
	shotResult NVARCHAR(50),
	xGoal NVARCHAR(50),
	positionX NVARCHAR(50),
	positionY NVARCHAR(50)
) ;


IF OBJECT_ID('bronze.src_teams','U') IS NOT NULL
	DROP TABLE bronze.src_teams ;
CREATE TABLE bronze.src_teams(
	teamID NVARCHAR(50),
	name NVARCHAR(100)
) ;


IF OBJECT_ID('bronze.src_teamstats','U') IS NOT NULL
	DROP TABLE bronze.src_teamstats ;
CREATE TABLE bronze.src_teamstats(
	gameID NVARCHAR(50),
	teamID NVARCHAR(50),
	season NVARCHAR(50),
	date NVARCHAR(100),
	location NVARCHAR(50),
	goals NVARCHAR(50),
	xGoals NVARCHAR(50),
	shots NVARCHAR(50),
	shotsOnTarget NVARCHAR(50),
	deep NVARCHAR(50),
	ppda NVARCHAR(50),
	fouls NVARCHAR(50),
	corners NVARCHAR(50),
	yellowCards NVARCHAR(50),
	redCards NVARCHAR(50),
	result NVARCHAR(50)
) ;