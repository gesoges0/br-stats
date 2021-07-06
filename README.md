# NBA-stats

## What is this?
Build MySQL database to analyze NBA stats.

You can build stats database such as [BASKETBALL REFERENCE](https://www.basketball-reference.com/players/j/jokicni01/gamelog/2021).
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
		# If you analyze triple double in regular season of all times, execute this script to output csv that is acceptable in flourish.
python analysis/HISTORY_TplDbl.py
```
![output](./images/HISTORY_TplDbl.gif)
