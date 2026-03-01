/*
=============================================================
Gold Layer: Data Integration Checks
=============================================================
Script Purpose:
    This script validates the integrity and correctness of the
    gold layer views after they have been created.
    It ensures that data has been correctly aggregated and
    integrated from the silver layer into the gold layer.

Checks Performed:
    1. Row Count Checks    : Gold should have fewer rows than
                             silver due to aggregation
    2. NULL Key Checks     : No NULLs allowed in key columns
    3. Totals Match        : Aggregated totals in gold must
                             match raw totals in silver
    4. Referential Checks  : All IDs in facts must exist in dims

Expected Results:
    - Checks 1,3 : Compare the two numbers manually
    - Checks 2,4 : Should return 0 rows or 0 count
=============================================================
*/

-- 1. Row count checks (Gold should have less than Silver due to aggregation)
SELECT COUNT(*) FROM silver.src_appearances  
SELECT COUNT(*) FROM gold.fact_playerstats   

-- 2. No NULLs in key columns
SELECT COUNT(*) FROM gold.fact_playerstats WHERE playerID IS NULL
SELECT COUNT(*) FROM gold.fact_teamstats WHERE teamID IS NULL
SELECT COUNT(*) FROM gold.fact_leaguetable WHERE leagueID IS NULL

-- 3. Totals match between Silver and Gold(should be equal)
SELECT SUM(goals) FROM silver.src_appearances          
SELECT SUM(TotalGoals) FROM gold.fact_playerstats      

-- 4. All IDs exist in dims(should return nothing)
SELECT f.playerID FROM gold.fact_playerstats f
LEFT JOIN gold.dim_players p ON f.playerID = p.playerID
WHERE p.playerID IS NULL  

-- 5. All teams in fact exist in dim( should return nothing)
SELECT f.teamID FROM gold.fact_teamstats f
LEFT JOIN gold.dim_teams t ON f.teamID = t.teamID
WHERE t.teamID IS NULL 

-- 6. Points calculation check
-- (TotalWins*3 + TotalDraws should equal Points)

SELECT teamID, season, leagueID, Points,
    TotalWins * 3 + TotalDraws AS ExpectedPoints
FROM gold.fact_leaguetable
WHERE Points != TotalWins * 3 + TotalDraws


-- 7. Games count check
-- (TotalWins + TotalDraws + TotalLosses should equal TotalGames)

SELECT teamID, season, leagueID, TotalGames,
    TotalWins + TotalDraws + TotalLosses AS ExpectedGames
FROM gold.fact_leaguetable
WHERE TotalGames != TotalWins + TotalDraws + TotalLosses


-- 8. All leagueIDs in facts exist in dim_leagues

SELECT DISTINCT p.leagueID FROM gold.fact_playerstats p
LEFT JOIN gold.dim_leagues l ON p.leagueID = l.leagueID
WHERE l.leagueID IS NULL

SELECT DISTINCT ts.leagueID FROM gold.fact_teamstats ts
LEFT JOIN gold.dim_leagues l ON ts.leagueID = l.leagueID
WHERE l.leagueID IS NULL


-- 9. All seasons in facts exist in dim_seasons

SELECT DISTINCT f.season FROM gold.fact_playerstats f
LEFT JOIN gold.dim_seasons s ON f.season = s.season
WHERE s.season IS NULL

SELECT DISTINCT f.season FROM gold.fact_teamstats f
LEFT JOIN gold.dim_seasons s ON f.season = s.season
WHERE s.season IS NULL