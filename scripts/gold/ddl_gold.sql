/*
=============================================================
DDL: Gold Layer Views
=============================================================
Script Purpose:
    This script creates all business-ready views in the 'gold'
    schema following a Galaxy Schema (Star Schema) design.
    Views are built on top of the silver layer and perform
    data integration, aggregations, and business logic.
    If a view already exists, it is dropped and recreated.

Views Created:
    Dimensions:
        - gold.dim_players   : Player info with most played position
        - gold.dim_leagues   : League info and Understat notation
        - gold.dim_teams     : Team identity info
        - gold.dim_seasons   : Season reference with formatted names

    Facts:
        - gold.fact_games        : Match results and goals per game
        - gold.fact_playerstats  : Aggregated player stats per season per league
        - gold.fact_teamstats    : Aggregated team stats per season per league
        - gold.fact_leaguetable  : Full league standings with points

Usage:
    Run this script after silver layer is loaded.
    Views can be queried directly for analytics and reporting.
=============================================================
*/

--======================
-- CREATING dim players
--======================
IF OBJECT_ID('gold.dim_players', 'V') IS NOT NULL
    DROP VIEW gold.dim_players;
GO
CREATE VIEW gold.dim_players AS
WITH player_positions AS (
    SELECT 
        playerID,
        position,
        ROW_NUMBER() OVER (PARTITION BY playerID ORDER BY COUNT(*) DESC) AS rn
    FROM silver.src_appearances
	WHERE position !='sub'
    GROUP BY playerID, position
)
SELECT 
    p.playerID,
    p.name PlayerName,
    pp.position AS MainPosition
FROM silver.src_players p
LEFT JOIN player_positions pp 
    ON p.playerID = pp.playerID 
    AND pp.rn = 1
GO
--=====================
-- CREATING dim leagues
--=====================
IF OBJECT_ID('gold.dim_leagues', 'V') IS NOT NULL
    DROP VIEW gold.dim_leagues;
GO

CREATE VIEW gold.dim_leagues AS
SELECT 
	l.leagueID,
	l.name leagueName,
	l.understatNotation
FROM silver.src_leagues l
GO
--======================
-- CREATING dim teams
--======================
IF OBJECT_ID('gold.dim_teams', 'V') IS NOT NULL
    DROP VIEW gold.dim_teams;
GO

CREATE VIEW gold.dim_teams AS
SELECT 
	t.teamID,
	t.name TeamName
FROM silver.src_teams t
GO
--======================
-- CREATING dim seasons
--======================
IF OBJECT_ID('gold.dim_seasons', 'V') IS NOT NULL
    DROP VIEW gold.dim_seasons;
GO

CREATE VIEW gold.dim_seasons AS
SELECT DISTINCT
	season,
	CAST(season AS VARCHAR(4)) + '/' + 
    CAST(season + 1 AS VARCHAR(4)) AS SeasonName
FROM silver.src_games
GO
--===========================
-- CREATING fact games
--===========================
IF OBJECT_ID('gold.fact_games','V') IS NOT NULL
	DROP VIEW gold.fact_games;
GO 
CREATE VIEW gold.fact_games AS
SELECT 
	g.gameID,
	g.leagueID,
	g.season,
	g.date,
	g.homeTeamID,
	g.awayTeamID,
	g.homeGoals,
	g.awayGoals,
	g.homeGoalsHalfTime,
	g.awayGoalsHalfTime
FROM silver.src_games g
GO
--================================
-- CREATING fact playerstats
--================================
IF OBJECT_ID('gold.fact_playerstats','V') IS NOT NULL
	DROP VIEW gold.fact_playerstats;
GO 
CREATE VIEW gold.fact_playerstats AS
SELECT 
	a.playerID,
	a.leagueID,
	g.season,
	SUM(a.goals) AS TotalGoals,
	SUM(a.assists) AS TotalAssists,
	SUM(a.ownGoals) AS TotalOwnGoals,
	SUM(a.keyPasses) AS TotalKeyPasses,
	SUM(a.shots) AS TotalShots,
	SUM(a.yellowCard) AS TotalYellowCards,
	SUM(a.redCard) AS TotalRedCards,
	COUNT(a.gameid) AS TotalGamesPlayed
FROM silver.src_appearances a
LEFT JOIN silver.src_games g
ON  a.gameID=g.gameID
GROUP BY a.playerID,a.leagueID,g.season
GO
--===============================
-- CREATING fact teamstats
--===============================
IF OBJECT_ID('gold.fact_teamstats','V') IS NOT NULL
	DROP VIEW gold.fact_teamstats;
GO 
CREATE VIEW gold.fact_teamstats AS
SELECT 
	ts.teamID,
	ts.season,
	g.leagueID,
	SUM(ts.goals) AS TotalGoals,
	SUM(ts.shots) AS TotalShots,
	SUM(ts.shotsOnTarget) AS TotalShotsOnTarget,
	SUM(ts.corners) AS TotalCorners,
	SUM(ts.fouls) AS TotalFouls,
	SUM(ts.yellowCards) AS TotalYellowCards,
	SUM(ts.redCards) AS TotalRedCards,
	SUM(CASE WHEN ts.goals = 0 THEN 1 ELSE 0 END) AS TotalCleanSheets,
	SUM(ts.goals) - SUM(CASE WHEN ts.location = 'home' THEN g.awayGoals 
							WHEN ts.location = 'away' THEN g.homeGoals 
							ELSE 0 END) AS GoalDifference,
	SUM(CASE WHEN ts.result = 'win' THEN 1 ELSE 0 END) AS TotalWins,
	SUM(CASE WHEN ts.result = 'draw' THEN 1 ELSE 0 END) AS TotalDraws,
    SUM(CASE WHEN ts.result = 'loss' THEN 1 ELSE 0 END) AS TotalLosses 
FROM silver.src_teamstats ts
LEFT JOIN silver.src_games g
ON g.gameID=ts.gameID
GROUP BY ts.teamID,ts.season,g.leagueID
GO
--==============================
-- CREATING fact league table
--==============================
IF OBJECT_ID('gold.fact_leaguetable','V') IS NOT NULL
	DROP VIEW gold.fact_leaguetable;
GO 
CREATE VIEW gold.fact_leaguetable AS
SELECT 
	ts.teamID,
	ts.season,
	g.leagueID,
	t.name AS TeamName,
	l.name AS LeagueName,
	COUNT(*) AS TotalGames,
	SUM(ts.goals) AS TotalGoals,
	SUM(ts.goals) - SUM(CASE WHEN ts.location = 'home' THEN g.awayGoals 
							WHEN ts.location = 'away' THEN g.homeGoals 
							ELSE 0 END) AS GoalDifference,
	SUM(CASE WHEN ts.location = 'home' THEN g.awayGoals 
			WHEN ts.location = 'away' THEN g.homeGoals 
			ELSE 0 END) AS GoalsAgainst,
	SUM(CASE WHEN ts.result = 'win' THEN 1 ELSE 0 END) AS TotalWins,
	SUM(CASE WHEN ts.result = 'draw' THEN 1 ELSE 0 END) AS TotalDraws,
    SUM(CASE WHEN ts.result = 'loss' THEN 1 ELSE 0 END) AS TotalLosses,
	SUM(CASE WHEN ts.result = 'win' THEN 3 
			WHEN ts.result = 'draw' THEN 1
			WHEN ts.result = 'loss' THEN 0 END ) AS Points
FROM silver.src_teamstats ts
LEFT JOIN silver.src_games g
ON g.gameID=ts.gameID
LEFT JOIN silver.src_teams t
ON ts.teamID = t.teamID
LEFT JOIN silver.src_leagues l
ON l.leagueID=g.leagueID
GROUP BY ts.teamID,ts.season,g.leagueID,t.name,l.name
GO



