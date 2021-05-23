DROP DATABASE NBA_overview_test;
CREATE DATABASE NBA_overview_test;
CREATE TABLE NBA_overview_test.all_players LIKE NBA3.all_players;
INSERT INTO NBA_overview_test.all_players SELECT * FROM NBA3.all_players;