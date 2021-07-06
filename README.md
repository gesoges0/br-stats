# NBA-stats

## What is this?
Build MySQL database to analyze NBA stats.

You can build stats database such a [BASKETBALL REFERENCE](https://www.basketball-reference.com/players/j/jokicni01/gamelog/2021).
![MySQL](./images/stats_info.png)

## Usage
### Update Database
```
# all players 
python all_players.py

# overview
python each_player_overview.py

# gamelog
python each_player_gamelog.py

# advanced gamelog
python each_player_advanced_gamelog.py
```

### Analyze Stats
```
python analysis/HISTORY_TplDbl.py
```
![output](./images/HISTORY_TplDbl.gif)
