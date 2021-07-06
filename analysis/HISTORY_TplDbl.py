# NBA3/analysis 上で実行する必要がある

import sys
import os
import csv
from typing import List, NamedTuple
from dataclasses import dataclass
import collections

# 日付関係
import datetime
from datetime import date
ONE_DAY = datetime.timedelta(days=1)

# DB関係
sys.path.append(os.pardir)
from each_player_overview import TotalsRecordRegularSeason, TotalsRecordPlayOffs
from each_player_gamelog import RegularSeasonRecord
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
    _from: int
    _to: int
    team: str
    stats_list: List
    stats_sum_list: List
    team_list: List

    def initialize_stats_list(self):
        self.stats_list = [0.0 for _ in range(self._to - self._from)]
        self.stats_sum_list = [0.0 for _ in range(self._to - self._from)]
        self.team_list = ['' for _ in range(self._to - self._from)]

    def calc_stats_sum_list(self):
        self.stats_sum_list[0] = self.stats_list[0]
        for i in range(1, self._to - self._from):
            self.stats_sum_list[i] = self.stats_sum_list[i-1] + self.stats_list[i]

    def get_calced_stats_sum_list_for_bar_chart(self):
        return [0.0] + self.stats_sum_list

    def set_team(self, type: str = 'longest_belonged'):
        if type == 'longest_belonged':
            team_counter_dict = dict(collections.Counter(self.team_list))
            del team_counter_dict['']
            self.team = max([(cnt, tm) for tm, cnt in team_counter_dict.items()])[1]
        if type == 'last_belonged':
            tmp = [tm for tm in self.team_list if tm] # ''を除去
            if tmp:
                self.team = tmp[-1] # 1つは所属しているはず
            else:
                self.team = ''


def export_history_TplDbl():
    _from = 1947
    _to = 2021

    # all_players tableにある全ての選手を初期化
    player_dict = dict()
    session = create_session('NBA3')
    for record in session.query(AllPlayersRecord).all():
        image_url = ''
        for _, in session.query(AllPlayerImageRecord._image_url).filter(AllPlayerImageRecord.id == record.id).all():
            if _: image_url = _
        player = Player(record.id, record._player, image_url, _from, _to, '', [], [], [])
        player.initialize_stats_list()
        player_dict[player.id] = player

    # Totals ( RegularSeason/ PlayOffs )tableを読み込んでいく
    session = create_session('NBA_overview_test')
    for year in range(_from, _to):
        season = f'{year}-{str(year+1)[2:]}'
        
        # レギュラーシーズン
        for record in session.query(TotalsRecordRegularSeason).filter(TotalsRecordRegularSeason._Season == season).all():
            if '_TrpDbl' not in record.__dict__.keys():
                continue
            player_dict[record.id].stats_list[year - _from] = getattr(record, '_TrpDbl') if getattr(record,'_TrpDbl') else 0
            player_dict[record.id].team_list[year - _from] = getattr(record, '_Tm') if getattr(record, '_Tm') else ''
        
        # プレイオフ
        # for record in session.query(TotalsRecordPlayOffs).filter(TotalsRecordPlayOffs._Season == season).all():
        #     if '_TrpDbl' not in record.__dict__.keys():
        #         continue
        #     player_dict[record.id].stats_list[year - _from] += getattr(record, '_TrpDbl') if getattr(record,'_TrpDbl') else 0
    
    # 各プレイヤーに対して合計値を更新する
    for id, player in player_dict.items():        
        player.calc_stats_sum_list()
        player.set_team(type='last_belonged')
    

    # 結果を出力
    output_file = './output/HISTORY_TplDbl.csv'
    session = create_session('NBA3')

    # HEADER = ['player_name', 'player_image_url', 'team'] + ['before'] + [f'Season {year}-{str(year+1)[2:]}' for year in range(_from, _to)]
    HEADER = ['player_name', 'player_image_url', 'team'] + [f'Season {_from}-{str(_from)[2:]}'] + [f'Season {year}-{str(year+1)[2:]}' for year in range(_from, _to)]
    with open(output_file, 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(HEADER)
        for id, player in player_dict.items():
            row = [player.player_name, player.image_url , player.team] + player.get_calced_stats_sum_list_for_bar_chart()
            writer.writerow(row)







    
    
    
    
    
    
if __name__ == '__main__':
    export_history_TplDbl()
    
    
    
    
    
    
    
    
    
    
    
#     # 2020-21 レギュラーシーズンの「得点」
#     _from = 1940
#     _to = 2021

#     HEADER = ['player_name', 'player_profile_link', 'team'] + [''] +  [_ for _ in range(_to - _from)]
#     players_dict = dict()
#     for year in range(_from, _to):
#         # 対象シーズン
#         season = f'{year}-{str(year)[2:]}'

        
#         for rs_gamelog_record in session.query(TotalsRecordRegularSeason).filter(TotalsRecordRegularSeason._Season == season).all():
#             # 初めてのプレイヤーであったら初期化
#             if rs_gamelog_record.id not in players_dict:
                
#                 player = Player(rs_gamelog_record.id, player_name, image_url, _from, _to, '', [], [], [])
#                 player.initialize_stats_list()
#                 players_dict[gamelog_record.id] = player





#     # この期間にレコードがあるプレイーを宣言していく
#     players_dict = dict()
#     for gamelog_record in session.query(RegularSeasonRecord).filter(_from <= RegularSeasonRecord._Date, RegularSeasonRecord._Date <= _to).all():
#         # 初めてのプレイヤーであったら初期化
#         if gamelog_record.id not in players_dict:
#             for res in session.execute(f'SELECT _player, _image_url FROM NBA3.all_player__images WHERE id={gamelog_record.id};'):
#                 player_name, image_url = res
#             player = Player(gamelog_record.id, player_name, image_url, _from, _to, '', [], [], [])
#             player.initialize_stats_list()
#             players_dict[gamelog_record.id] = player

#         # gamelogをplayerに追加
#         if gamelog_record._PTS is None:
#             pts = 0
#         else:
#             pts = gamelog_record._PTS
#         players_dict[gamelog_record.id].stats_list[dayindex(_from, gamelog_record._Date)] = pts
#         players_dict[gamelog_record.id].team_list[dayindex(_from, gamelog_record._Date)] = gamelog_record._Tm


#     # プレイヤーの総合を計算
#     for id, player in players_dict.items():
#         player.calc_stats_sum_list()

#     # チームを決定
#     for id, player in players_dict.items():
#         player.set_team()

#     # csv へ出力
#     output_file_path = './output/2020_21_regular_season_PTS.csv'
#     with open(output_file_path, 'w') as f:
#         writer = csv.writer(f, lineterminator='\n')
#         writer.writerow(HEADER)
#         for id, player in players_dict.items():
#             row_for_output = [player.player_name, player.image_url, player.team] + player.get_calced_stats_sum_list_for_bar_chart()
#             writer.writerow(row_for_output)



# if __name__ == '__main__':
#     export_2020_21_regular_season_PTS()
