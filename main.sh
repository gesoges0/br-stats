# all_player
python3 all_players.py
python3 all_player_picture.py

# チーム
python3 all_teams.py

# overview
python3 each_player_overview.py

# gamelog
# NBA_gamelog内にPerGameRecordに相当するtableをNBA_overviewからコピーする必要があります
# CREATE TABLE NBA_gamelog.each_player_overview__per_game__regular_season LIKE NBA_overview_test.each_player_overview__per_game__regular_season;
# INSERT INTO NBA_gamelog.each_player_overview__per_game__regular_season SELECT * FROM NBA_overview_test.each_player_overview__per_game__regular_season;
python3 each_player_gamelog.py
# advanced gamelogはgamelogに記録された最後の日に出たプレイヤーを収集します
# 定期的にmain上の収集規則で取り逃したかもしれないレコードを収集しましょう
python3 each_player_advanced_gamelog.py



# backup 
mkdir backup01
mysqldump --single-transaction -u gesogeso -p NBA3 > backup/NBA3.dump
mysqldump --single-transaction -u gesogeso -p NBA_teams > backup/NBA_teams.dump
mysqldump --single-transaction -u gesogeso -p NBA_overview_test > backup/NBA_overview_test.dump
mysqldump --single-transaction -u gesogeso -p NBA_gamelog > backup/NBA_gamelog.dump

