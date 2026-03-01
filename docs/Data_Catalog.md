# Data Catalog — Football Data Warehouse (Gold Layer)

> **Source:** Understat / Kaggle (Technika148)  
> **Database:** SQL Server  
> **Layer:** Gold (Views)  
> **Schema:** `gold`

---

## Table of Contents
- [dim_players](#dim_players)
- [dim_teams](#dim_teams)
- [dim_leagues](#dim_leagues)
- [dim_seasons](#dim_seasons)
- [fact_games](#fact_games)
- [fact_playerstats](#fact_playerstats)
- [fact_teamstats](#fact_teamstats)
- [fact_leaguetable](#fact_leaguetable)

---

## dim_players

**Description:** Contains player identity information including their most frequently played position.  
**Source:** `silver.src_players` + `silver.src_appearances`  
**Object Type:** View

| Column | Data Type | Description |
|---|---|---|
| playerID | INT | Unique identifier for the player (PK) |
| PlayerName | NVARCHAR | Full name of the player |
| MainPosition | NVARCHAR | Most frequently played position (excludes substitute appearances) |

---

## dim_teams

**Description:** Contains team identity information.  
**Source:** `silver.src_teams`  
**Object Type:** View

| Column | Data Type | Description |
|---|---|---|
| teamID | INT | Unique identifier for the team (PK) |
| TeamName | NVARCHAR | Full name of the team |

---

## dim_leagues

**Description:** Contains league identity and reference information.  
**Source:** `silver.src_leagues`  
**Object Type:** View

| Column | Data Type | Description |
|---|---|---|
| leagueID | INT | Unique identifier for the league (PK) |
| leagueName | NVARCHAR | Full name of the league |
| understatNotation | NVARCHAR | League identifier used by Understat website (e.g. EPL, La_liga) |

---

## dim_seasons

**Description:** Contains season reference data with formatted season names.  
**Source:** `silver.src_games`  
**Object Type:** View

| Column | Data Type | Description |
|---|---|---|
| season | INT | Season start year (e.g. 2015) (PK) |
| SeasonName | VARCHAR | Formatted season name (e.g. 2015/2016) |

---

## fact_games

**Description:** Contains one row per match with goals and match details.  
**Source:** `silver.src_games`  
**Object Type:** View  
**Grain:** One row per game

| Column | Data Type | Description |
|---|---|---|
| gameID | INT | Unique identifier for the game (PK) |
| leagueID | INT | FK → dim_leagues |
| season | INT | FK → dim_seasons |
| date | DATETIME | Date the match was played |
| homeTeamID | INT | FK → dim_teams (home team) |
| awayTeamID | INT | FK → dim_teams (away team) |
| homeGoals | INT | Goals scored by the home team |
| awayGoals | INT | Goals scored by the away team |
| homeGoalsHalfTime | INT | Home team goals at half time |
| awayGoalsHalfTime | INT | Away team goals at half time |

---

## fact_playerstats

**Description:** Aggregated player statistics per season per league.  
**Source:** `silver.src_appearances` + `silver.src_games`  
**Object Type:** View  
**Grain:** One row per player per league per season

| Column | Data Type | Description |
|---|---|---|
| playerID | INT | FK → dim_players |
| leagueID | INT | FK → dim_leagues |
| season | INT | FK → dim_seasons |
| TotalGoals | INT | Total goals scored |
| TotalAssists | INT | Total assists made |
| TotalOwnGoals | INT | Total own goals scored |
| TotalKeyPasses | INT | Total key passes made |
| TotalShots | INT | Total shots taken |
| TotalYellowCards | INT | Total yellow cards received |
| TotalRedCards | INT | Total red cards received |
| TotalGamesPlayed | INT | Total number of games played |

---

## fact_teamstats

**Description:** Aggregated team performance statistics per season per league.  
**Source:** `silver.src_teamstats` + `silver.src_games`  
**Object Type:** View  
**Grain:** One row per team per league per season

| Column | Data Type | Description |
|---|---|---|
| teamID | INT | FK → dim_teams |
| season | INT | FK → dim_seasons |
| leagueID | INT | FK → dim_leagues |
| TotalGoals | INT | Total goals scored |
| TotalShots | INT | Total shots taken |
| TotalShotsOnTarget | INT | Total shots on target |
| TotalCorners | INT | Total corner kicks |
| TotalFouls | INT | Total fouls committed |
| TotalYellowCards | INT | Total yellow cards received |
| TotalRedCards | INT | Total red cards received |
| TotalCleanSheets | INT | Number of games where team conceded 0 goals |
| GoalDifference | INT | Goals scored minus goals conceded |
| TotalWins | INT | Total number of wins |
| TotalDraws | INT | Total number of draws |
| TotalLosses | INT | Total number of losses |

---

## fact_leaguetable

**Description:** Complete league standings table with points and rankings per season.  
**Source:** `silver.src_teamstats` + `silver.src_games` + `silver.src_teams` + `silver.src_leagues`  
**Object Type:** View  
**Grain:** One row per team per league per season  
**Sort Order:** leagueID → season → Points DESC → GoalDifference DESC → TotalGoals DESC

| Column | Data Type | Description |
|---|---|---|
| teamID | INT | FK → dim_teams |
| season | INT | FK → dim_seasons |
| leagueID | INT | FK → dim_leagues |
| TeamName | NVARCHAR | Name of the team |
| LeagueName | NVARCHAR | Name of the league |
| TotalGames | INT | Total games played in the season |
| TotalGoals | INT | Total goals scored (Goals For) |
| GoalDifference | INT | Goals scored minus goals conceded |
| GoalsAgainst | INT | Total goals conceded |
| TotalWins | INT | Total wins |
| TotalDraws | INT | Total draws |
| TotalLosses | INT | Total losses |
| Points | INT | Total points (Win=3, Draw=1, Loss=0) |
