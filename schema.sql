-- Play-by-play data for NBA games (2024-25, 2025-26 seasons)
-- Primary key is composite: gameId + actionId uniquely identifies each play
CREATE TABLE nba_data (
    gameId VARCHAR,
    clock VARCHAR,
    period INT,
    teamId INT,
    personId INT,
    playerName VARCHAR,
    xLegacy FLOAT,
    yLegacy FLOAT,
    shotDistance FLOAT,
    shotResult VARCHAR,
    isFieldGoal INT,
    scoreHome INT,
    scoreAway INT,
    pointsTotal INT,
    location VARCHAR,
    description VARCHAR,
    actionType VARCHAR,
    shotValue INT,
    actionId INT,
    PRIMARY KEY(gameId, actionId)
)