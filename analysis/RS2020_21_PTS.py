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
DB_NAME = 'NBA_gamelog'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))



def dayrange(_from, _to, step=1):
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

def dayadd(_day, _diff: int):
    res = date.fromisoformat(_day)
    for i in range(_diff):
        res += ONE_DAY
    return res.strftime('%Y-%m-%d')

def daysub(_day, _diff: int):
    res = date.fromisoformat(_day)
    for i in range(_diff):
        res -= ONE_DAY
    return res.strftime('%Y-%m-%d')

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
        self.stats_list = [0.0 for _ in dayrange(self._from, self._to)]
        self.stats_sum_list = [0.0 for _ in dayrange(self._from, self._to)]
        self.team_list = ['' for _ in dayrange(self._from, self._to)]

    def calc_stats_sum_list(self):
        self.stats_sum_list[0] = self.stats_list[0]
        for i in range(1, dayindex(self._from, self._to)):
            self.stats_sum_list[i] = self.stats_sum_list[i-1] + self.stats_list[i]

    def get_calced_stats_sum_list_for_bar_chart(self):
        return [0.0] + self.stats_sum_list

    def set_team(self):
        team_counter_dict = dict(collections.Counter(self.team_list))
        del team_counter_dict['']
        self.team = max([(cnt, tm) for tm, cnt in team_counter_dict.items()])[1]


def export_2020_21_regular_season_PTS():
    # 2020-21 レギュラーシーズンの「得点」
    _from = '2020-12-22'
    _to = '2021-05-17'

    HEADER = ['player_name', 'player_profile_link', 'team'] + [_from] + [_ for _ in dayrange(_from, _to)]

    # この期間にレコードがあるプレイーを宣言していく
    players_dict = dict()
    for gamelog_record in session.query(RegularSeasonRecord).filter(_from <= RegularSeasonRecord._Date, RegularSeasonRecord._Date <= _to).all():
        # 初めてのプレイヤーであったら初期化
        if gamelog_record.id not in players_dict:
            for res in session.execute(f'SELECT _player, _image_url FROM NBA3.all_player__images WHERE id={gamelog_record.id};'):
                player_name, image_url = res
            player = Player(gamelog_record.id, player_name, image_url, _from, _to, '', [], [], [])
            player.initialize_stats_list()
            players_dict[gamelog_record.id] = player

        # gamelogをplayerに追加
        if gamelog_record._PTS is None:
            pts = 0
        else:
            pts = gamelog_record._PTS
        players_dict[gamelog_record.id].stats_list[dayindex(_from, gamelog_record._Date)] = pts
        players_dict[gamelog_record.id].team_list[dayindex(_from, gamelog_record._Date)] = gamelog_record._Tm


    # プレイヤーの総合を計算
    for id, player in players_dict.items():
        player.calc_stats_sum_list()

    # チームを決定
    for id, player in players_dict.items():
        player.set_team()

    # csv へ出力
    output_file_path = './output/2020_21_regular_season_PTS.csv'
    with open(output_file_path, 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(HEADER)
        for id, player in players_dict.items():
            row_for_output = [player.player_name, player.image_url, player.team] + player.get_calced_stats_sum_list_for_bar_chart()
            writer.writerow(row_for_output)



if __name__ == '__main__':
    export_2020_21_regular_season_PTS()
