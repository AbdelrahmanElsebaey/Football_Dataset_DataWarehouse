/*
=============================================================
Silver Layer: Data Quality Checks
=============================================================
Script Purpose:
    This script performs data quality checks on all bronze
    layer tables before loading into the silver layer.
    It covers the following checks:
    - NULL checks
    - Negative value checks
    - Referential integrity checks
    - Business rule validations
    - Duplicate checks
    - Data type validity checks
    - Categorical value checks

Tables Checked:
    - bronze.src_appearances
    - bronze.src_leagues
    - bronze.src_players
    - bronze.src_games
    - bronze.src_teams
    - bronze.src_shots
    - bronze.src_teamstats

Note:
    Queries returning 0 rows indicate no issues found.
    Queries returning rows indicate data quality issues
    that should be investigated before loading to silver.
=============================================================
*/

--=============================
--(1)checking appearances table
--=============================

--****************************************************************************
/*checking nulls in gameID,playerID columns + checking wheather they are found
  on their tables (gameID in games table,playerID in players table) */
  --****************************************************************************
SELECT 
	a.gameID,
	a.playerID,
	CASE 
        WHEN a.gameID IS NULL THEN 'NULL gameID'
        WHEN a.playerID IS NULL THEN 'NULL playerID'
        WHEN g.gameID IS NULL THEN 'gameID not in games'
        WHEN p.playerID IS NULL THEN 'playerID not in players'
    END AS issue
FROM bronze.src_appearances a
LEFT JOIN bronze.src_games g
ON a.gameID=g.gameID
LEFT JOIN bronze.src_players p
ON a.playerID=p.playerID
WHERE a.gameID IS NULL 
	OR a.playerID IS NULL 
	OR g.gameID  IS NULL
	OR p.playerID  IS NULL

--***********************************************************************
--checking wheather there is negative numbers on gameID,playerID columns
--***********************************************************************

SELECT 
	CAST(gameID AS INT) gameID,
	CAST(playerID AS INT) playerID
FROM bronze.src_appearances
WHERE gameID < 0 OR playerID < 0

--checking if leagueID not found in leagues table

SELECT a.leagueID FROM bronze.src_appearances a
LEFT JOIN bronze.src_leagues l
ON a.leagueID=l.leagueID
WHERE l.leagueID IS NULL OR a.leagueID IS NULL

--****************************************************************************
--checking wheather goals are negative and wheather it contains decimals,nulls
--****************************************************************************

SELECT goals FROM bronze.src_appearances
WHERE TRY_CAST(goals AS INT) < 0 OR TRY_CAST(goals AS INT) IS NULL

SELECT goals
FROM bronze.src_appearances
WHERE TRY_CAST(goals AS INT) IS NOT NULL  
  AND goals LIKE '%.%'  

 --******************************************************************************
--checking wheather owngoals are negative and wheather it contains decimals,nulls
 --******************************************************************************

SELECT owngoals FROM bronze.src_appearances
WHERE TRY_CAST(owngoals AS INT) < 0 OR TRY_CAST(owngoals AS INT) IS NULL

SELECT owngoals
FROM bronze.src_appearances
WHERE TRY_CAST(owngoals AS INT) IS NOT NULL  
  AND owngoals LIKE '%.%' 
 
 --*******************************************************
--checking if shots are negative or decimal,contain nulls
--check  if there is goals>shots, THE NORMAL IS goals <=shots
 --*******************************************************

SELECT shots FROM bronze.src_appearances
WHERE TRY_CAST(shots AS INT) < 0 OR TRY_CAST(shots AS INT) IS NULL

SELECT shots
FROM bronze.src_appearances
WHERE TRY_CAST(shots AS INT) IS NOT NULL  
  AND shots LIKE '%.%' 

SELECT TRY_CAST(shots AS INT) shots,TRY_CAST(goals AS INT) goals FROM bronze.src_appearances
WHERE TRY_CAST(shots AS INT)  < TRY_CAST(goals AS INT)

 --*************************************************************************
--check if xgoals contains nulls, negatives, check xGoals should be <= shots
 --*************************************************************************

SELECT TRY_CAST(xgoals AS FLOAT) xgoals,TRY_CAST(shots AS INT) shots
FROM bronze.src_appearances
WHERE TRY_CAST(xgoals AS FLOAT) IS NULL
OR TRY_CAST(xgoals AS FLOAT) < 0
OR TRY_CAST(xgoals AS FLOAT) > TRY_CAST(shots AS INT)

 --*************************************************************************
--check if xgoalschain contains nulls, negatives, 
/*check xGoalschain should be >= xgoals
(in this dataset there are approx 30,000 rows have xgoals < xgoals chain 
so i flaged it )*/
 --*************************************************************************

SELECT TRY_CAST(xgoals AS FLOAT) xgoals,TRY_CAST(xgoalschain AS FLOAT) xgoalschain,
    CASE
        WHEN TRY_CAST(xGoalsChain AS FLOAT) IS NULL THEN 'NULL xGoalsChain'
        WHEN TRY_CAST(xGoalsChain AS FLOAT) < 0 THEN 'negative xGoalsChain'
        WHEN TRY_CAST(xGoals AS FLOAT) > TRY_CAST(xGoalsChain AS FLOAT) THEN 'xGoals > xGoalsChain'
    END AS issue
FROM bronze.src_appearances
WHERE TRY_CAST(xgoalschain AS FLOAT) IS NULL
OR TRY_CAST(xgoalschain AS FLOAT) < 0
OR TRY_CAST(xgoals AS FLOAT)  > TRY_CAST(xgoalschain AS FLOAT)

--checking the avg differences

/*SELECT 
    AVG(TRY_CAST(xGoals AS FLOAT)) AS avg_xg,
    AVG(TRY_CAST(xGoalsChain AS FLOAT)) AS avg_xg_chain,
	AVG(TRY_CAST(xGoalsChain AS FLOAT)) - AVG(TRY_CAST(xGoals AS FLOAT)) as diff
FROM bronze.src_appearances
*/

 --***************************************************
--check if xgoalsbuildup contains(nulls,neagtives)
--check ((xGoalsBuildup <= xGoalsChain))-->normal
 --***************************************************

SELECT 
	TRY_CAST(xgoalsbuildup AS FLOAT) xgoalsbuildup,
	TRY_CAST(xgoalschain AS FLOAT) xgoalschain,
	CASE 
		WHEN TRY_CAST(xgoalsbuildup AS FLOAT) IS NULL THEN 'NULL' 
		WHEN TRY_CAST(xgoalsbuildup AS FLOAT) < 0 THEN 'NEGATIVE'
		WHEN TRY_CAST(xgoalschain AS FLOAT) < TRY_CAST(xgoalsbuildup AS FLOAT)
		THEN 'xGoalsBuildup > xGoalsChain' END ISSUE_INDENTIFIER
FROM bronze.src_appearances
WHERE TRY_CAST(xgoalsbuildup AS FLOAT) IS NULL
OR TRY_CAST(xgoalsbuildup AS FLOAT) < 0
OR TRY_CAST(xgoalschain AS FLOAT) < TRY_CAST(xgoalsbuildup AS FLOAT)

 --*********************************************
--Check if assists is negative,contains nulls
 --*********************************************

SELECT 
	TRY_CAST(assists AS INT) assists
FROM bronze.src_appearances
WHERE TRY_CAST(assists AS INT) < 0
OR TRY_CAST(assists AS INT) IS NULL

 --*************************************************************************
--Check if keypasses is negative,contains nulls
--keypasses >=assists IS THE NORMAL ,SO check if there is opposite to that
 --*************************************************************************

SELECT
	TRY_CAST(keypasses AS INT) keypasses,
	TRY_CAST(assists AS INT) assists,
	CASE	
		WHEN TRY_CAST(keypasses AS INT) IS NULL THEN 'NULL'
		WHEN TRY_CAST(keypasses AS INT) < 0 THEN 'NEGATIVE'
		WHEN TRY_CAST(keypasses AS INT) < TRY_CAST(assists AS INT) 
		THEN 'keypasses < assists' END issue_identifier
FROM bronze.src_appearances
WHERE TRY_CAST(keypasses AS INT) IS NULL
OR TRY_CAST(keypasses AS INT) < 0
OR TRY_CAST(keypasses AS INT) < TRY_CAST(assists AS INT)

 --*******************************
--check xassists nulls,negatives
 --*******************************

SELECT
	TRY_CAST(xassists AS FLOAT) xassists
FROM bronze.src_appearances
WHERE TRY_CAST(xassists AS FLOAT) < 0
OR TRY_CAST(xassists AS FLOAT) IS NULL

 --********************************************
--check position column (nulls,valid values)
 --********************************************

--check the position values
SELECT DISTINCT position FROM bronze.src_appearances

SELECT
	position
FROM bronze.src_appearances
WHERE position IS NULL
OR LOWER(position) NOT IN 
    ('aml', 'amr', 'dc', 'fwl', 'mc', 'gk', 
    'fwr', 'dr', 'dmc', 'dl', 'dmr', 'fw', 
    'sub', 'amc', 'dml', 'ml', 'mr') --copied from the result of code above

 --********************************************
--positionorder column(check nulls,validity)
 --********************************************

--CHECK the values
SELECT distinct CAST(TRIM(positionOrder) AS INT) positionorder
FROM bronze.src_appearances
ORDER BY CAST(TRIM(positionOrder) AS INT)

SELECT distinct CAST(TRIM(positionOrder) AS INT) positionorder
FROM bronze.src_appearances
WHERE CAST(TRIM(positionOrder) AS INT) < 1
OR CAST(TRIM(positionOrder) AS INT) > 17
OR TRY_CAST(TRIM(positionOrder) AS INT) IS NULL

--*******************************************************************************
--yellow cards,red cards check(nulls,negatives,more than 2 yellow , or 2 red card)
--*******************************************************************************

WITH yellow_red_cards AS (
    SELECT
        TRY_CAST(yellowCard AS INT) AS yellowCard,
        TRY_CAST(redCard AS INT) AS redCard
    FROM bronze.src_appearances
)
SELECT *
FROM yellow_red_cards
WHERE yellowCard IS NULL
   OR yellowCard < 0
   OR yellowCard > 2
   OR redCard IS NULL
   OR redCard < 0
   OR redCard > 1;

--*****************************************************************
--time check(nulls,negatives,if more tha 90 min(the match time) )
--*****************************************************************

WITH time_check AS (
	SELECT 
	TRY_CAST(time as int) time
	FROM bronze.src_appearances
)
SELECT *
FROM time_check
WHERE time > 90
OR time IS NULL
OR time < 0

--****************************************
--sub in,sub out,CHECK NULLS,negatives
--****************************************
SELECT 
    MIN(substituteIn),
    MAX(substituteIn)
FROM bronze.src_appearances;


SELECT substituteIn,substituteOut FROM bronze.src_appearances
WHERE TRY_CAST(substituteIn AS INT) IS NULL OR TRY_CAST(substituteIn AS INT) < 0
OR TRY_CAST(substituteOUT AS INT) IS NULL OR TRY_CAST(substituteIn AS INT) < 0
--===================================================
--checking leagues table
--(the whole table is 5 rows no need for hard checks)
--===================================================

--**********************************
--leagueID
--(CHECK NULLS,NEGATIVES,DUPLICATES)
--**********************************

SELECT
	TRY_CAST(leagueID as int) leagueID
FROM bronze.src_leagues
WHERE TRY_CAST(leagueID as int) IS NULL
OR TRY_CAST(leagueID as int) < 0

SELECT
	TRY_CAST(leagueID as int) leagueID,
	COUNT(*) COUNTING_IDs_DUPLICATES
FROM bronze.src_leagues
GROUP BY TRY_CAST(leagueID as int)
HAVING COUNT(*)>1

--====================
--players table check
--====================

--playersID check(nulls,negatives,duplicates)

SELECT
	TRY_CAST(playerID AS INT) playerID
FROM bronze.src_players
WHERE TRY_CAST(playerID AS INT) IS NULL 
OR TRY_CAST(playerID AS INT) < 0

SELECT
    TRY_CAST(playerID AS INT) playerID,
    COUNT(*) duplicate_count
FROM bronze.src_players
GROUP BY TRY_CAST(playerID AS INT)
HAVING COUNT(*) > 1

--name column check 

SELECT name
FROM bronze.src_players
WHERE name IS NULL OR TRIM(name) ='' OR name LIKE '%[#@$!*\/&%^()]%' --found some strange strings in output as '&#039;'

--replace(IN HTML ENCODING '&#039;' IS EQUAL TO ' )
SELECT REPLACE(name,'&#039;', '''') NAME,cast(playerID as int)
FROM bronze.src_players
WHERE  REPLACE(name,'&#039;', '''') LIKE '%[#@$!*\/&%^()]%'


--===========================
--games table 
--===========================

--gameID,leagueID(check nulls , negatives wheather leagueid not in leagueid table)
WITH league_game_ids AS(
	SELECT 
		TRY_CAST(leagueID as int) leagueID,
		TRY_CAST(gameID as int) gameID
	FROM bronze.src_games
)

SELECT *
FROM league_game_ids g
LEFT JOIN bronze.src_leagues l
ON g.leagueID=l.leagueID
WHERE g.leagueID IS NULL OR g.leagueID < 0 OR l.leagueID IS NULL
OR g.gameID IS NULL OR g.gameID < 0


--season

--to check what years are included
SELECT DISTINCT season FROM bronze.src_games

--check nulls,neagtives
SELECT TRY_CAST(season AS INT) season
FROM bronze.src_games
WHERE TRY_CAST(season AS INT)  is null 
OR TRY_CAST(season AS INT) < 0
OR TRY_CAST(season AS INT) > 2020
OR TRY_CAST(season AS INT) < 2014

------------------
--date
------------------

--CHECK NULLS,FUTURE DATES
SELECT
	TRY_CAST(date as datetime) date 
FROM bronze.src_games
WHERE TRY_CAST(date as datetime) > GETDATE()
OR TRY_CAST(date as datetime) IS NULL

--CHECK UNREALISTIC DATES
SELECT 
    MIN(TRY_CAST(date AS DATETIME)) AS min_date,
    MAX(TRY_CAST(date AS DATETIME)) AS max_date
FROM bronze.src_games


-----------------------
--hometeamID,awayteamID
-----------------------

--CHECK nulls,neagtives, or not found in teams table
WITH home_away_Tid AS(
	SELECT
		TRY_CAST(hometeamID as INT) hometeamID,
		TRY_CAST(awayteamID as INT) awayteamID
	FROM bronze.src_games
)
SELECT * FROM home_away_Tid as cte
LEFT JOIN bronze.src_teams ht
ON cte.hometeamID = ht.teamID 
LEFT JOIN bronze.src_teams at 
ON cte.awayteamID = at.teamID
WHERE cte.hometeamID IS NULL OR cte.hometeamID < 0
OR cte.awayteamID IS NULL OR cte.awayteamID < 0
OR ht.teamID is null or at.teamID is null
OR cte.hometeamID = cte.awayteamID


--home,away,draw probability table
--CHECK NULLS,BETWEEN 0 AND 1 , HOME PROB. + AWAY PROB. + DRAW PROB MUST BE =1
WITH CTE_PROB AS (
	SELECT 
		TRY_CAST(homeprobability AS FLOAT) homeprobability,
		TRY_CAST(awayprobability AS FLOAT) awayprobability,
		TRY_CAST(drawprobability AS FLOAT) drawprobability
	FROM bronze.src_games
)
SELECT * FROM CTE_PROB
WHERE homeprobability > 1 OR homeprobability < 0 OR homeprobability IS NULL
OR awayprobability > 1 OR awayprobability < 0 OR awayprobability IS NULL
OR drawprobability >1  OR drawprobability < 0 OR drawprobability IS NULL
OR ABS((homeprobability + awayprobability + drawprobability) - 1) > 0.01

------------------
--home,away goals  
------------------

--check nulls,negatives

WITH cte_home_away_goals AS (
    SELECT
        TRY_CAST(homeGoals AS INT) homeGoals,
        TRY_CAST(awayGoals AS INT) awayGoals
    FROM bronze.src_games
)
SELECT * FROM cte_home_away_goals
WHERE homeGoals IS NULL OR homeGoals < 0
   OR awayGoals IS NULL OR awayGoals < 0
 
 --check for unrealistic number of goals
SELECT 
    MAX(TRY_CAST(homeGoals AS INT)) max_home,
    MAX(TRY_CAST(awayGoals AS INT)) max_away,
	MIN(TRY_CAST(awayGoals AS INT)) min_home,
    MIN(TRY_CAST(awayGoals AS INT)) min_away
FROM bronze.src_games

---------------------------
--home,away goals halftime
---------------------------

WITH cte_ht_goals AS (
    SELECT
        TRY_CAST(homeGoalsHalfTime AS INT) homeGoalsHalfTime,
        TRY_CAST(awayGoalsHalfTime AS INT) awayGoalsHalfTime,
        TRY_CAST(homeGoals AS INT) homeGoals,
        TRY_CAST(awayGoals AS INT) awayGoals
    FROM bronze.src_games
)
SELECT * FROM cte_ht_goals
WHERE homeGoalsHalfTime IS NULL OR homeGoalsHalfTime < 0
   OR awayGoalsHalfTime IS NULL OR awayGoalsHalfTime < 0
   OR homeGoalsHalfTime > homegoals
   OR awayGoalsHalfTime > awayGoals


--=====================
--teams table
--=====================

--teamID column

--null,negative check
SELECT 
	TRY_CAST(teamid AS INT) teamID
FROM bronze.src_teams
WHERE TRY_CAST(teamid AS INT) < 0
OR TRY_CAST(teamid AS INT) IS NULL

--duplicate check
SELECT TRY_CAST(teamid AS INT) teamID,COUNT(*) COUNT_ids
FROM bronze.src_teams
GROUP BY TRY_CAST(teamid AS INT) 
HAVING COUNT(*) > 1

--name column(check nulls,strange characters)

SELECT name FROM bronze.src_teams
WHERE name is null or name like '%[#@$!*\/&%^()]%'

--================================
--shots table
--================================

--gameID,shooterID,assisterID columns (check nulls,Referential Integrity)
WITH cte_shots AS(
	SELECT 
		TRY_CAST(gameID AS INT) gameID,
		TRY_CAST(shooterID AS INT) shooterID,
		TRY_CAST(assisterID AS INT) assisterID
	FROM bronze.src_shots
)

SELECT * FROM cte_shots s
LEFT JOIN bronze.src_games g
ON g.gameID=s.gameID
LEFT JOIN bronze.src_players ps
ON ps.playerID=s.shooterID
LEFT JOIN bronze.src_players pa
ON pa.playerID=s.assisterID
WHERE s.gameID IS NULL OR s.shooterID IS NULL
OR ps.playerID IS NULL 
OR s.gameID < 0  OR g.gameID IS NULL

--minute column check (NULLS,>120 OR <0)

SELECT
	TRY_CAST(minute AS INT) minute
FROM bronze.src_shots
WHERE minute > 120 OR minute < 0 OR minute IS NULL

--situation column check

-- to see possible values
SELECT DISTINCT situation FROM bronze.src_shots
--its only 5 values to need for validity checks

--last action column

-- to see possible values
SELECT DISTINCT lastAction FROM bronze.src_shots

--check nulls , empty str
SELECT  lastAction FROM bronze.src_shots
WHERE lastAction is null or trim(lastAction) =''

--shottype,shot result columns

--check their possible values
SELECT  DISTINCT shottype FROM bronze.src_shots
SELECT  DISTINCT shotresult FROM bronze.src_shots
--result few rows both columns so no need for validity checks

--xgoal column,POSITIONX,positiony (check nulls , check >1 or <0 for xgoals)
-- the position range is betw 0 and 100
WITH CTE_floatvals AS (
	SELECT 
		TRY_CAST(xgoal AS FLOAT) xgoal,
		TRY_CAST(positionX AS FLOAT) positionX,
		TRY_CAST(positionY AS FLOAT) positionY
	FROM bronze.src_shots
)
SELECT * FROM CTE_floatvals
WHERE xgoal IS NULL OR xgoal < 0 OR xgoal > 1
OR positionX > 100 OR positionX < 0 OR positionX IS NULL
OR positionY > 100 OR positionY < 0 OR positionY  IS NULL


--=====================
--teamstats table
--=====================

--gameid,teamid,season columns check (nulls,negatives,season range betw 2014 and 2020)
WITH cte_teamstats AS (
	SELECT 
		TRY_CAST(gameID AS INT) gameID,
		TRY_CAST(teamID AS INT) teamID,
		TRY_CAST(season AS INT) season
	FROM bronze.src_teamstats
)

SELECT * FROM cte_teamstats ts
LEFT JOIN bronze.src_games g
ON g.gameID=ts.gameID
LEFT JOIN bronze.src_teams t
ON t.teamID=ts.teamID
WHERE ts.gameID IS NULL OR ts.gameID < 0 OR g.gameID IS NULL
OR ts.teamID IS NULL OR ts.teamID < 0 OR t.teamID IS NULL
OR ts.season IS NULL OR ts.season < 0 OR ts.season > 2020 OR ts.season < 2014

--date column

--check for nulls ,future dates
SELECT TRY_CAST(date AS datetime) date FROM bronze.src_teamstats
WHERE date > getdate() OR date is null

--to check the date range
SELECT 
    MIN(TRY_CAST(date AS DATETIME)) min_date,
    MAX(TRY_CAST(date AS DATETIME)) max_date
FROM bronze.src_teamstats

--location column

-- null check
SELECT location FROM bronze.src_teamstats
WHERE location IS NULL
--to check possible values
SELECT  distinct location FROM bronze.src_teamstats
--2 values only so no need for validity check

--goals column check nulls ,negatives,refrential integrity

SELECT TRY_CAST(goals AS INT) goals FROM bronze.src_teamstats 
WHERE TRY_CAST(goals AS INT) IS NULL OR TRY_CAST(goals AS INT) < 0

SELECT ts.gameID, ts.teamID, ts.goals, g.homeGoals, g.awayGoals
FROM bronze.src_teamstats ts
LEFT JOIN bronze.src_games g ON TRY_CAST(ts.gameID AS INT) = TRY_CAST(g.gameID AS INT)
WHERE (ts.location = 'h' AND TRY_CAST(ts.goals AS FLOAT) != TRY_CAST(g.homeGoals AS FLOAT))
   OR (ts.location = 'a' AND TRY_CAST(ts.goals AS FLOAT) != TRY_CAST(g.awayGoals AS FLOAT))

--xgoals check nulls,negatives

SELECT TRY_CAST(xGoals AS FLOAT) xGoals
FROM bronze.src_teamstats
WHERE TRY_CAST(xGoals AS FLOAT) IS NULL
   OR TRY_CAST(xGoals AS FLOAT) < 0

--shots check nulls,negatives,decimals
SELECT shots FROM bronze.src_teamstats
WHERE TRY_CAST(shots AS INT) IS NULL
   OR TRY_CAST(shots AS INT) < 0
   OR shots LIKE '%.%'

--shotsontarget check nulls,negatives,decimals,check no shotsontarget>shots
SELECT shotsOnTarget FROM bronze.src_teamstats
WHERE TRY_CAST(shotsOnTarget AS INT) IS NULL
   OR TRY_CAST(shotsOnTarget AS INT) < 0
   OR shotsOnTarget LIKE '%.%'

SELECT shotsOnTarget, shots FROM bronze.src_teamstats
WHERE TRY_CAST(shotsOnTarget AS INT) > TRY_CAST(shots AS INT)

--deep,ppda columns check nulls,negatives
SELECT deep FROM bronze.src_teamstats
WHERE TRY_CAST(deep AS INT) IS NULL
   OR TRY_CAST(deep AS INT) < 0

SELECT ppda FROM bronze.src_teamstats
WHERE TRY_CAST(ppda AS FLOAT) IS NULL
   OR TRY_CAST(ppda AS FLOAT) < 0

--fouls check nulls,negatives
SELECT fouls FROM bronze.src_teamstats
WHERE TRY_CAST(fouls AS INT) IS NULL
   OR TRY_CAST(fouls AS INT) < 0

--corners column check nulls,negatives
SELECT corners FROM bronze.src_teamstats
WHERE TRY_CAST(corners AS INT) IS NULL
   OR TRY_CAST(corners AS INT) < 0


--yellowcards column check nulls,negatives
SELECT yellowCards FROM bronze.src_teamstats
WHERE TRY_CAST(yellowCards AS INT) IS NULL
   OR TRY_CAST(yellowCards AS INT) < 0

--redCards column check nulls,negatives
SELECT redCards FROM bronze.src_teamstats
WHERE TRY_CAST(redCards AS INT) IS NULL
   OR TRY_CAST(redCards AS INT) < 0

--result column
--NULL CHECK
SELECT result FROM bronze.src_teamstats
where result IS NULL 
--check possible values
SELECT DISTINCT TRIM(LOWER(result)) result FROM bronze.src_teamstats
--result 3 values no need for validity checks