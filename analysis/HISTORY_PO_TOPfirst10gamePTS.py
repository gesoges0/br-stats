# https://twitter.com/NBAHistory/status/1398854000385142789
# PO最初の10試合でとった得点のランキング



import sys
import os
import csv
from typing import List, NamedTuple
from dataclasses import dataclass
import collections

# 日付関係
import datetime
from datetime import date

from sqlalchemy.sql.expression import select
ONE_DAY = datetime.timedelta(days=1)

# DB関係
sys.path.append(os.pardir)
from each_player_overview import TotalsRecordRegularSeason, TotalsRecordPlayOffs
from each_player_gamelog import RegularSeasonRecord, PlayoffsRecord
from all_players import AllPlayersRecord
from all_player_picture import AllPlayerImageRecord
from dataclasses import dataclass

# db
import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Text, DateTime, Sequence, ForeignKey, create_engine, MetaData, DECIMAL, DATETIME, exc, event, Index
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from utils import ALPHABET, HOST, DB_NAME
from utils import get_mysql_pass, get_soup_by_url

Base = declarative_base()
USER, PASSWD = get_mysql_pass()

def create_session(db_name):
    DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{db_name}?charset=utf8'
    ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
    session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))
    return session



def yearrange(_from, _to, step=1):
    """ YYYY-mm-ddフォーマットの日付を一日づつ進めていく関数
    """
    _from = date.fromisoformat(_from)
    _to = date.fromisoformat(_to)
    while _from < _to:
        res = _from.strftime('%Y-%m-%d')
        _from += ONE_DAY
        yield res

def dayindex(_from, target):
    """ YYYY-mm-ddフォーマットの日付のtargetが_fromから何日離れているかを計算
    """
    diff = date.fromisoformat(target) - date.fromisoformat(_from)
    return diff.days

@dataclass
class Player:
    id: int
    player_name: str
    image_url: str
    team: str
    stats_list: List
    stats_sum_list: List
    team_list: List

    def __lt__(self, other):
        return self.player_name < other.player_name
    
    def __gt__(self, other):
        return self.player_name > other.player_name

    def __eq__(self, other):
        return self.player_name == other.player_name

    def initialize_stats_list(self, _first_N_stats):
        self.stats_list = _first_N_stats
        self.stats_sum_list = [_first_N_stats[0]] + [0 for _ in range(len(_first_N_stats)-1)]
        for i in range(1, len(self.stats_list)):
            self.stats_sum_list[i] = self.stats_sum_list[i-1] + self.stats_list[i]
    
        



def export_history_PO_first10game_PTS(stats_type = '_PTS', num_first_game=3):
    # all_players tableにある全ての選手を初期化
    player_dict = dict()
    session = create_session('NBA3')
    for record in session.query(AllPlayersRecord).all():
        image_url = ''
        for _, in session.query(AllPlayerImageRecord._image_url).filter(AllPlayerImageRecord.id == record.id).all():
            if _: image_url = _
        player = Player(record.id, record._player, image_url, '', [], [], [])
        player_dict[player.id] = player

    
    # N回未満の出場選手を候補から外す
    session = create_session('NBA_gamelog')
    delete_candidates = set()
    # for id, player in player_dict.items():
    #     # Totals ( RegularSeason/ PlayOffs )tableを読み込んでいく
    #     _gamelog_records = list(session.query(PlayoffsRecord)\
    #         .filter(PlayoffsRecord.id == player.id, PlayoffsRecord._G != None, PlayoffsRecord._PTS != None)\
    #         .order_by(PlayoffsRecord._Date))
    #     if len(_gamelog_records) < num_first_game:
    #         delete_candidates.add(id)
    
    # for candidate_id in delete_candidates:
    #     del player_dict[candidate_id]
    
    # statts初期化
    for id, player in player_dict.items():
        _gamelog_records = list(session.query(PlayoffsRecord)\
            .filter(PlayoffsRecord.id == player.id, PlayoffsRecord._G != None, PlayoffsRecord._PTS != None)\
            .order_by(PlayoffsRecord._Date))
        
        # 最初のN試合のNULLではないstats
        if len(_gamelog_records) > num_first_game:
            _first_N_stats = [getattr(_, stats_type) for _ in _gamelog_records][:num_first_game]
        else:
            _first_N_stats = [getattr(_, stats_type) for _ in _gamelog_records] + [0 for _ in range(num_first_game - len(_gamelog_records))]
        
        
        player.initialize_stats_list(_first_N_stats)
    
    # stats sort
    results_list = []
    for id, player in player_dict.items():
        result = ([player.stats_sum_list[-1]], player)
        results_list.append(result)
    
    results_list.sort(reverse=True)
    
    # result
    OUTPUT_PATH = f'output/first_{num_first_game}_G_NBA_PTS.csv'
    HEADER = ['player_name', 'player_image_url'] + ['0'] + [f'{year}' for year in range(1, 11)]
    with open(OUTPUT_PATH, 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(HEADER)
        for i, (sum_stats_type, player) in enumerate(results_list):
            row = [player.player_name, player.image_url] + [0] + player.stats_sum_list
            writer.writerow(row)
    

    OUTPUT_OPTION_PATH = OUTPUT_PATH.replace('.csv', '_option.txt')
    N = 10
    with open(OUTPUT_OPTION_PATH, 'w') as f:

        for _ in range(num_first_game):
            _from = _ + 1
            _to = _ + 2

            # Top {N} に入っており, その年で最もスコアが高い人
            _stat_yaer = [p for s, p in sorted(results_list, key=lambda x : x[1].stats_list[_], reverse=True)]
            _stats_all = [p for s, p in sorted(results_list, key=lambda x : x[1].stats_sum_list[_], reverse=True)][:N]

            _year_result = []
            for p in _stat_yaer:
                if p in _stats_all:
                    _year_result.append(p.player_name)

            f.write('-----------------------------\n')
            f.write(f'{_from}-{_to}\n')
            f.write(f'{_year_result}\n')
            print('-----------------------------')
            print(f'{_from}-{_to}')
            print(f'{_year_result}')
            


            
            

        


    
    
    
if __name__ == '__main__':
    export_history_PO_first10game_PTS(stats_type='_PTS', num_first_game=10)
    
    
    
    
    
    
    
    
    
    