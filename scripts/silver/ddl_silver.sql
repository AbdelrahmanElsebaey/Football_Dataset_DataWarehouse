/*
=============================================================
DDL: Silver Layer Tables
=============================================================
Script Purpose:
    This script creates all cleaned and standardized tables
    in the 'silver' schema. Unlike the bronze layer, columns
    are defined with proper data types after cleaning.
    If a table already exists, it is dropped and recreated.

Transformations Applied:
    - Proper data types (INT, FLOAT, DATETIME, NVARCHAR)
    - Primary key constraints defined
    - FLAG column added to src_appearances for data quality tracking

Tables Created:
    - silver.src_appearances
    - silver.src_games
    - silver.src_leagues
    - silver.src_players
    - silver.src_shots
    - silver.src_teams
    - silver.src_teamstats

WARNING:
    Running this script will drop and recreate all silver tables.
    All existing data in the silver layer will be permanently deleted.
=============================================================
*/

PRINT '============================='
PRINT 'CREATING SILVER LAYER TABLES'
PRINT '============================='

IF OBJECT_ID('silver.src_appearances','U') IS NOT NULL
	DROP TABLE silver.src_appearances ;
CREATE TABLE silver.src_appearances(
	gameID INT NOT NULL,
	playerID INT NOT NULL,
	goals INT,
	ownGoals INT,
	shots INT,
	xGoals FLOAT,
	xGoalsChain FLOAT,
	FLAG NVARCHAR(50),
	xGoalsBuildup FLOAT,
	assists INT,
	keyPasses INT,
	xAssists FLOAT,
	position NVARCHAR(50),
	positionOrder INT,
	yellowCard INT,
	redCard INT,
	time INT ,
	substituteIn INT,
	substituteOut INT,
	leagueID INT,
	CONSTRAINT PK_src_appearances PRIMARY KEY (gameID, playerID)
) ;

IF OBJECT_ID('silver.src_games','U') IS NOT NULL
	DROP TABLE silver.src_games ;
CREATE TABLE silver.src_games(
	gameID	INT NOT NULL PRIMARY KEY,
	leagueID INT,
	season INT,
	date DATETIME,
	homeTeamID INT,
	awayTeamID INT,
	homeGoals INT,
	awayGoals INT,
	homeProbability FLOAT,
	drawProbability FLOAT,
	awayProbability FLOAT,
	homeGoalsHalfTime INT,
	awayGoalsHalfTime INT
) ;

IF OBJECT_ID('silver.src_leagues','U') IS NOT NULL
	DROP TABLE silver.src_leagues ;
CREATE TABLE silver.src_leagues(
	leagueID INT NOT NULL PRIMARY KEY,
	name NVARCHAR(50),
	understatNotation NVARCHAR(50)
);

IF OBJECT_ID('silver.src_players','U') IS NOT NULL
	DROP TABLE silver.src_players ;
CREATE TABLE silver.src_players(
	playerID INT NOT NULL PRIMARY KEY,
	name NVARCHAR(100)
) ;


IF OBJECT_ID('silver.src_shots','U') IS NOT NULL
	DROP TABLE silver.src_shots ;
CREATE TABLE silver.src_shots(
	gameID INT NOT NULL,
	shooterID INT NOT NULL,
	assisterID INT,
	minute INT ,
	situation NVARCHAR(50),
	lastAction NVARCHAR(50),
	shotType NVARCHAR(50),
	shotResult NVARCHAR(50),
	xGoal FLOAT,
	positionX FLOAT,
	positionY FLOAT
) ;


IF OBJECT_ID('silver.src_teams','U') IS NOT NULL
	DROP TABLE silver.src_teams ;
CREATE TABLE silver.src_teams(
	teamID INT NOT NULL PRIMARY KEY,
	name NVARCHAR(100)
) ;


IF OBJECT_ID('silver.src_teamstats','U') IS NOT NULL
	DROP TABLE silver.src_teamstats ;
CREATE TABLE silver.src_teamstats(
	gameID INT NOT NULL,
	teamID INT NOT NULL,
	season INT,
	date DATETIME,
	location NVARCHAR(50),
	goals INT,
	xGoals FLOAT,
	shots INT,
	shotsOnTarget INT,
	deep INT,
	ppda FLOAT,
	fouls INT,
	corners INT,
	yellowCards INT,
	redCards INT,
	result NVARCHAR(50),
	CONSTRAINT PK_src_teamstats PRIMARY KEY (gameID, teamID)
) ;
