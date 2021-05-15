import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Text, DateTime, Sequence, ForeignKey, create_engine, MetaData, DECIMAL, DATETIME, exc, event, Index
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.ext.declarative import declarative_base

import requests
from bs4 import BeautifulSoup
from pathlib import Path
from utils import ALPHABET, HOST, DB_NAME
from utils import get_mysql_pass, get_soup_by_url
from typing import Any, List

from all_players import AllPlayersRecord
import time

Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DB_NAME = 'NBA_overview_test'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

CURRENT_SEASON = "2020-21"

class EachPlayerOverViewPage():
    id: int
    url: str
    per_game_table: Any
    totals_table: Any

    def __init__(self, id, url):
        self.id = id
        self.url = url
        
    def create_tables(self, table_name):
        # if table_name == 'per_games':
        #     Base.metadata.create_all(bind=ENGINE, tables=[PerGameRecord.__table__])
        # if table_name == 'totals':
        #     Base.metadata.create_all(bind=ENGINE, tables=[TotalsRecord.__table__])
        Base.metadata.create_all(bind=ENGINE)

    def update_each_player_tables(self):
        # chromiumでスクレイピング
        soup = get_soup_by_url(self.url, False)
        # table_soups = soup.find_all('tbody')
        
        # CURRENT_SEASON は更新が入るため一旦削除
        session.query(PerGameRecordRegularSeason).filter(PerGameRecordRegularSeason.id == self.id, PerGameRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(PerGameRecordPlayOffs).filter(PerGameRecordPlayOffs.id == self.id, PerGameRecordPlayOffs._Season == CURRENT_SEASON).delete()
        session.query(TotalsRecordRegularSeason).filter(TotalsRecordRegularSeason.id == self.id, TotalsRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(TotalsRecordPlayOffs).filter(TotalsRecordPlayOffs.id == self.id, TotalsRecordPlayOffs._Season == CURRENT_SEASON).delete()
        session.commit()
        
        # per game
        # regular season
        per_game_soup = soup.find('div', id='div_per_game')
        if per_game_soup:
            per_games_table_soup = per_game_soup.find('tbody')
            per_game_table = PerGameTable(self.id, per_games_table_soup, 'regular_season')
            for per_game_record in per_game_table.get_records():
                if not session.query(PerGameRecordRegularSeason.id, PerGameRecordRegularSeason._Season).filter(PerGameRecordRegularSeason.id == per_game_record.id, PerGameRecordRegularSeason._Season == per_game_record._Season, PerGameRecordRegularSeason._Tm == per_game_record._Tm).first():
                    session.add(per_game_record)
                    session.commit()

        # playoffs
        playoff_soup = soup.find('div', id='div_playoffs_per_game')
        if playoff_soup:
            per_games_table_soup = playoff_soup.find('tbody')
            per_game_table = PerGameTable(self.id, per_games_table_soup, 'playoffs')
            for per_game_record in per_game_table.get_records():
                if not session.query(PerGameRecordPlayOffs.id, PerGameRecordPlayOffs._Season).filter(PerGameRecordPlayOffs.id == per_game_record.id, PerGameRecordPlayOffs._Season == per_game_record._Season, PerGameRecordPlayOffs._Tm == per_game_record._Tm).first():
                    print(per_game_record.__dict__)
                    session.add(per_game_record)
                    session.commit()
                
        # totals
        # regular season
        per_game_soup = soup.find('div', id='div_totals')
        if per_game_soup:
            totals_table_soup = per_game_soup.find('tbody')
            totals_table = TotalsTable(self.id, totals_table_soup, 'regular_season')
            for totals_record in totals_table.get_records():
                if not session.query(TotalsRecordRegularSeason.id, TotalsRecordRegularSeason._Season).filter(TotalsRecordRegularSeason.id == totals_record.id, TotalsRecordRegularSeason._Season == totals_record._Season, TotalsRecordRegularSeason._Tm == totals_record._Tm).first():
                    session.add(totals_record)
                    session.commit()
        
        # playoffs
        playoffs_soup = soup.find('div', id='div_playoffs_totals')
        if playoffs_soup:
            totals_table_soup = playoffs_soup.find('tbody')
            totals_table = TotalsTable(self.id, totals_table_soup, 'playoffs')
            for totals_record in totals_table.get_records():
                if not session.query(TotalsRecordPlayOffs.id, TotalsRecordPlayOffs._Season).filter(TotalsRecordPlayOffs.id == totals_record.id, TotalsRecordPlayOffs._Season == totals_record._Season, TotalsRecordPlayOffs._Tm == totals_record._Tm).first():
                    session.add(totals_record)
                    session.commit()

class PerGameTable():
    table: Any
    id: int
    season_type: str

    def __init__(self, id, soup, season_type):
        self.id = id
        self.table = soup
        self.season_type = season_type

    def get_records(self):
        for _tr in self.table.find_all('tr'):
            if not _tr.find('th'):
                continue
            # statsがあってもSeasonの欄がhrefでない場合対策
            if _tr.find('th').find('a'):
                _Season = _tr.find('th').find('a').text
            else: 
                _Season = _tr.find('th').text
            # injuryなど
            if not _tr.find_all('td'):
                continue
            _td_list = [_.text for _ in _tr.find_all('td')]
            # _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS = _td_list
            # per_game_record = PerGameRecord(_Season, _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS)
            stats = dict()
            keys_17 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST','_PF', '_PTS']
            keys_29 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
            keys_22 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
            if len(_td_list) == 29:
                # _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS = _td_list
                stats = {k: v for k, v in zip(keys_29, _td_list)}
            if len(_td_list) == 22:
                stats = {k: v for k, v in zip(keys_22, _td_list)}
                # _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS = _td_list
                # _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent = -1, -1, -1, -1, -1, -1, -1
            if len(_td_list) == 17:
                stats = {k: v for k, v in zip(keys_17, _td_list)}
            stats['id'] = self.id
            stats['_Season'] = _Season
            # スタッツが空白対策
            del_target_set = set()
            for stats_type, stats_val in stats.items():
                if stats_val != 0. and not stats_val:
                    del_target_set.add(stats_type)
            for del_target in del_target_set:
                del stats[del_target]
            
            # チームはPKなので無かったらcontinue
            if '_Tm' not in stats:
                continue
            
            if self.season_type == 'regular_season':
                per_game_record = PerGameRecordRegularSeason(**stats)
            else:
                per_game_record = PerGameRecordPlayOffs(**stats)
            yield per_game_record

class TotalsTable():
    table: Any
    id: int
    season_type: str

    def __init__(self, id, soup, season_type):
        self.id = id
        self.table = soup
        self.season_type = season_type

    def get_records(self):
        for _tr in self.table.find_all('tr'):
            if not _tr.find('th'):
                continue
            # statsがあってもSeasonの欄がhrefでない場合対策
            if _tr.find('th').find('a'):
                _Season = _tr.find('th').find('a').text
            else: 
                _Season = _tr.find('th').text
            # injuryなど
            if not _tr.find_all('td'):
                continue
            _td_list = [_.text for _ in _tr.find_all('td')]
            stats = dict()
            keys_17 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST','_PF', '_PTS']
            keys_22 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
            keys_29 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
            keys_31 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_TrpDbl']
            if len(_td_list) == 29:
                # _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS = _td_list
                stats = {k: v for k, v in zip(keys_29, _td_list)}
            if len(_td_list) == 22:
                stats = {k: v for k, v in zip(keys_22, _td_list)}
                # _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS = _td_list
                # _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent = -1, -1, -1, -1, -1, -1, -1
            if len(_td_list) >= 30:
                stats = {k: v for k, v in zip(keys_31, _td_list)}
            if len(_td_list) == 17:
                stats = {k: v for k, v in zip(keys_17, _td_list)}
            stats['id'] = self.id
            stats['_Season'] = _Season
            # スタッツが空対策
            del_target_set = set()
            for stats_type, stats_val in stats.items():
                if stats_val != 0. and not stats_val:
                    del_target_set.add(stats_type)
            for del_target in del_target_set:
                del stats[del_target]

            # チームはPKなので無かったらcontinue
            if '_Tm' not in stats:
                continue
            # TrpDblがある選手は1コマ明ける
            if '_tmp' in stats:
                del stats['_tmp']

            if self.season_type == 'regular_season':
                totals_record = TotalsRecordRegularSeason(**stats)
            else:
                totals_record = TotalsRecordPlayOffs(**stats)
            # totals_record = TotalsRecord(_Season, _Age, _Tm, _Lg, _Pos, _G, _GS, _MP, _FG, _FGA, _FG_percent, _3P, _3PA, _3P_percent, _2P, _2PA, _2P_percent, _eFG_percent, _FT, _FTA, _FT_percent, _ORB, _DRB, _TRB, _AST, _STL, _BLK, _TOV, _PF, _PTS)
            yield totals_record


class PerGameRecordRegularSeason(Base):
    __tablename__ = 'each_player_per_game_regular_season'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(4), primary_key=True)
    _Lg = Column(String(4))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Float)
    _FG = Column(Float)
    _FGA = Column(Float)
    _FG_percent = Column(Float)
    _3P = Column(Float)
    _3PA = Column(Float)
    _3P_percent = Column(Float)
    _2P = Column(Float)
    _2PA = Column(Float)
    _2P_percent = Column(Float)
    _eFG_percent = Column(Float)
    _FT = Column(Float)
    _FTA = Column(Float)
    _FT_percent = Column(Float)
    _ORB = Column(Float)
    _DRB = Column(Float)
    _TRB = Column(Float)
    _AST = Column(Float)
    _STL = Column(Float)
    _BLK = Column(Float)
    _TOV = Column(Float)
    _PF = Column(Float)
    _PTS = Column(Float)

class PerGameRecordPlayOffs(Base):
    __tablename__ = 'each_player_per_game_playoffs'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(4), primary_key=True)
    _Lg = Column(String(4))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Float)
    _FG = Column(Float)
    _FGA = Column(Float)
    _FG_percent = Column(Float)
    _3P = Column(Float)
    _3PA = Column(Float)
    _3P_percent = Column(Float)
    _2P = Column(Float)
    _2PA = Column(Float)
    _2P_percent = Column(Float)
    _eFG_percent = Column(Float)
    _FT = Column(Float)
    _FTA = Column(Float)
    _FT_percent = Column(Float)
    _ORB = Column(Float)
    _DRB = Column(Float)
    _TRB = Column(Float)
    _AST = Column(Float)
    _STL = Column(Float)
    _BLK = Column(Float)
    _TOV = Column(Float)
    _PF = Column(Float)
    _PTS = Column(Float)


    # def __init__(self, id, _Season, _Age=None, _Tm=None, _Lg=None, _Pos=None, _G=None, _GS=None, _MP=None, _FG=None, _FGA=None, _FG_percent=None, _FT=None, _FTA=None, _FT_percent=None, _ORB=None, _DRB=None, _TRB=None, _AST=None, _STL=None, _BLK=None, _TOV=None, _PF=None, _PTS=None, _3P=None, _3PA=None, _3P_percent=None, _2P=None, _2PA=None, _2P_percent=None, _eFG_percent=None):
    #     self.id = id
    #     self._Season = _Season
    #     self._Age = _Age
    #     self._Tm = _Tm
    #     self._Lg = _Lg
    #     self._Pos = _Pos
    #     self._G = _G
    #     self._GS = _GS
    #     self._MP = _MP
    #     self._FG = _FG
    #     self._FGA = _FGA
    #     self._FG_percent = _FG_percent
    #     self._3P = _3P
    #     self._3PA = _3PA
    #     self._3P_percent = _3P_percent
    #     self._2P = _2P
    #     self._2PA = _2PA
    #     self._2P_percent = _2P_percent
    #     self._eFG_percent = _eFG_percent
    #     self._FT = _FT
    #     self._FTA = _FTA
    #     self._FT_percent = _FT_percent
    #     self._ORB = _ORB
    #     self._DRB = _DRB
    #     self._TRB = _TRB
    #     self._AST = _AST
    #     self._STL = _STL
    #     self._BLK = _BLK
    #     self._TOV = _TOV
    #     self._PF = _PF
    #     self._PTS = _PTS   

class TotalsRecordRegularSeason(Base):
    __tablename__ = 'each_player_totals_regular_season'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(4), primary_key=True)
    _Lg = Column(String(4))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Integer)
    _FG = Column(Integer)
    _FGA = Column(Integer)
    _FG_percent = Column(Float)
    _3P = Column(Integer)
    _3PA = Column(Integer)
    _3P_percent = Column(Float)
    _2P = Column(Integer)
    _2PA = Column(Integer)
    _2P_percent = Column(Float)
    _eFG_percent = Column(Float)
    _FT = Column(Integer)
    _FTA = Column(Integer)
    _FT_percent = Column(Float)
    _ORB = Column(Integer)
    _DRB = Column(Integer)
    _TRB = Column(Integer)
    _AST = Column(Integer)
    _STL = Column(Integer)
    _BLK = Column(Integer)
    _TOV = Column(Integer)
    _PF = Column(Integer)
    _PTS = Column(Integer)
    _TrpDbl = Column(Integer)


class TotalsRecordPlayOffs(Base):
    __tablename__ = 'each_player_totals_playoffs'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(4), primary_key=True)
    _Lg = Column(String(4))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Integer)
    _FG = Column(Integer)
    _FGA = Column(Integer)
    _FG_percent = Column(Float)
    _3P = Column(Integer)
    _3PA = Column(Integer)
    _3P_percent = Column(Float)
    _2P = Column(Integer)
    _2PA = Column(Integer)
    _2P_percent = Column(Float)
    _eFG_percent = Column(Float)
    _FT = Column(Integer)
    _FTA = Column(Integer)
    _FT_percent = Column(Float)
    _ORB = Column(Integer)
    _DRB = Column(Integer)
    _TRB = Column(Integer)
    _AST = Column(Integer)
    _STL = Column(Integer)
    _BLK = Column(Integer)
    _TOV = Column(Integer)
    _PF = Column(Integer)
    _PTS = Column(Integer)
    _TrpDbl = Column(Integer)



if __name__ == '__main__':
    for index, player_overview_url in session.query(AllPlayersRecord.id, AllPlayersRecord._url).all():
        # time.sleep(4)
        print(index, player_overview_url)
        # 1人のプレイヤーを示す
        overview_page = EachPlayerOverViewPage(index, player_overview_url)
        overview_page.create_tables('per_games')
        overview_page.create_tables('totals')
        overview_page.update_each_player_tables()




# class AllPlayersPage():
#     def __init__(self):
#         pass

#     def create_table(self):
#         """tableを作成する"""
#         # Base.metadata.create_all(bind=ENGINE)
#         Base.metadata.create_all(bind=ENGINE, tables=[AllPlayersRecord.__table__])

#     def update_all_players_table(self):
#         """選手テーブルを更新する"""
#         for alpha in ALPHABET:
#             url = f'https://www.basketball-reference.com/players/{alpha}/'
#             all_players_table = AllPlayersTable(url)
#             for player in all_players_table.get_all_players_in_a_page():
#                 if session.query(AllPlayersRecord).filter(AllPlayersRecord._player == player._player).first():
#                     continue
#                 session.add(player)
#                 session.commit()

# class AllPlayersTable():
#     """Web側のtable"""
#     url: str
#     table: Any
#     all_players: List[Any]

#     def __init__(self, url):
#         self.url = url
#         # res = requests.get(self.url)
#         # soup = BeautifulSoup(res.text, 'html.parser')
#         soup = get_soup_by_url(self.url, False)
#         self.table = soup.find('tbody')

#     def get_all_players_in_a_page(self):
#         """Web側の一つのページにある選手情報を返すgenerator"""
#         for _tr in self.table.find_all('tr'):
#             if _tr.get('class'):
#                 continue
#             _th = _tr.find('th').find('a')
#             _player_url, _player_name = 'https://www.basketball-reference.com' + _th.get('href'), _th.text
#             _From, _To, _Pos, _Ht, _Wt, _Birth_Date, _Colleges = [_.text for _ in _tr.find_all('td')]
#             _url_for_player = _player_url.replace('.', '').replace('html', '').split('/')[-1]
#             player_record = AllPlayersRecord(_player_name, _From, _To, _Pos, _Ht, _Wt, _Birth_Date, _Colleges, _player_url, _url_for_player)
#             yield player_record

# class AllPlayersRecord(Base):
#     """
#     all_players tableの1recordを表す
#     https://www.basketball-reference.com/players/a/
#     """
#     __tablename__ = 'all_players'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     _player = Column(String(120), unique=True)
#     _from = Column(Integer)
#     _to = Column(Integer)
#     _pos = Column(String(10))
#     _ht = Column(String(10))
#     _wt = Column(Integer)
#     _birth_date = Column(String(36))
#     _colleges = Column(String(120))
#     _url = Column(String(120))
#     _url_for_player = Column(String(120))

#     def __init__(self, _player, _from, _to, _pos, _ht, _wt, _birth_date, _colleges, _url, _url_for_player):
#         self._player = _player
#         self._from = _from 
#         self._to = _to
#         self._pos = _pos
#         self._ht = _ht
#         self._wt = _wt
#         self._birth_date = _birth_date
#         self._colleges = _colleges
#         self._url = _url
#         self._url_for_player = _url_for_player