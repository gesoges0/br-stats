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
from dataclasses import dataclass

from all_players import AllPlayersRecord
import time

Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DB_NAME = 'NBA_overview_test'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))
CURRENT_SEASON = '2020-21'

def create_session(db_name):
    DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{db_name}?charset=utf8'
    ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
    session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))
    return session

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
        soup = get_soup_by_url(self.url, True)
        # table_soups = soup.find_all('tbody')
        
        # CURRENT_SEASON は更新が入るため一旦削除
        session.query(PerGameRecordRegularSeason).filter(PerGameRecordRegularSeason.id == self.id, PerGameRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(PerGameRecordPlayOffs).filter(PerGameRecordPlayOffs.id == self.id, PerGameRecordPlayOffs._Season == CURRENT_SEASON).delete()
        session.query(TotalsRecordRegularSeason).filter(TotalsRecordRegularSeason.id == self.id, TotalsRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(TotalsRecordPlayOffs).filter(TotalsRecordPlayOffs.id == self.id, TotalsRecordPlayOffs._Season == CURRENT_SEASON).delete()
        session.query(Per36MinutesRecordRegularSeason).filter(Per36MinutesRecordRegularSeason.id == self.id, Per36MinutesRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(Per36MinutesRecordPlayoffs).filter(Per36MinutesRecordPlayoffs.id == self.id, Per36MinutesRecordPlayoffs._Season == CURRENT_SEASON).delete()
        session.query(Per100PossRecordRegularSeason).filter(Per100PossRecordRegularSeason.id == self.id, Per100PossRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(Per100PossRecordPlayoffs).filter(Per100PossRecordPlayoffs.id == self.id, Per100PossRecordPlayoffs._Season == CURRENT_SEASON).delete()
        session.query(AdvancedRecordRegularSeason).filter(AdvancedRecordRegularSeason.id == self.id, AdvancedRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(AdvancedRecordPlayoffs).filter(AdvancedRecordPlayoffs.id == self.id, AdvancedRecordPlayoffs._Season == CURRENT_SEASON).delete()
        session.query(PlayByPlayRecordRegularSeason).filter(PlayByPlayRecordRegularSeason.id == self.id, PlayByPlayRecordRegularSeason._Season == CURRENT_SEASON).delete()
        session.query(PlayByPlayRecordPlayoffs).filter(PlayByPlayRecordPlayoffs.id == self.id, PlayByPlayRecordPlayoffs._Season == CURRENT_SEASON).delete()
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

        # Per36 Minutes
        # regular
        per_36_minutes_soup = soup.find('div', id='div_per_minute')
        if per_36_minutes_soup:        
            table_soup = per_36_minutes_soup.find('tbody')
            table = Per36MinutesTable(self.id, table_soup, 'regular_season')
            for record in table.get_records():
                if not session.query(Per36MinutesRecordRegularSeason).filter(Per36MinutesRecordRegularSeason.id==record.id, Per36MinutesRecordRegularSeason._Season==record._Season, Per36MinutesRecordRegularSeason._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()
                    
        # playoffs
        per_36_minutes_soup = soup.find('div', id='div_playoffs_per_minute')
        if per_36_minutes_soup:
            table_soup = per_36_minutes_soup.find('tbody')
            table = Per36MinutesTable(self.id, table_soup, 'playoffs')
            for record in table.get_records():
                if not session.query(Per36MinutesRecordPlayoffs).filter(Per36MinutesRecordPlayoffs.id==record.id, Per36MinutesRecordPlayoffs._Season==record._Season, Per36MinutesRecordPlayoffs._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()

        # Per 100 Poss
        # regular
        per_100_poss_soup = soup.find('div', id='div_per_poss')
        if per_100_poss_soup:
            table_soup = per_100_poss_soup.find('tbody')
            table = Per100PossTable(self.id, table_soup, 'regular_season')
            for record in table.get_records():
                if not session.query(Per100PossRecordRegularSeason).filter(Per100PossRecordRegularSeason.id==record.id, Per100PossRecordRegularSeason._Season==record._Season, Per100PossRecordRegularSeason._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()

        # playoffs
        per_100_poss_soup = soup.find('div', id='div_playoffs_per_poss')
        if per_100_poss_soup:
            table_soup = per_100_poss_soup.find('tbody')
            table = Per100PossTable(self.id, table_soup, 'playoffs')
            for record in table.get_records():
                if not session.query(Per100PossRecordPlayoffs).filter(Per100PossRecordPlayoffs.id==record.id, Per100PossRecordPlayoffs._Season==record._Season, Per100PossRecordPlayoffs._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()

        # Advanced
        # regular
        advanced_soup = soup.find('div', id='div_advanced')
        if advanced_soup:
            table_soup = advanced_soup.find('tbody')
            table = AdvancedTable(self.id, table_soup, 'regular_season')
            for record in table.get_records():
                if not session.query(AdvancedRecordRegularSeason).filter(AdvancedRecordRegularSeason.id==record.id, AdvancedRecordRegularSeason._Season==record._Season, AdvancedRecordRegularSeason._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()

        # playoffs
        advanced_soup = soup.find('div', id='div_playoffs_advanced')
        if advanced_soup:
            table_soup = advanced_soup.find('tbody')
            table = AdvancedTable(self.id, table_soup, 'playoffs')
            for record in table.get_records():
                if not session.query(AdvancedRecordPlayoffs).filter(AdvancedRecordPlayoffs.id==record.id, AdvancedRecordPlayoffs._Season==record._Season, AdvancedRecordPlayoffs._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()

        # Play-by-Play
        # regular
        pbp_soup = soup.find('div', id='div_pbp')
        if pbp_soup:
            table_soup = pbp_soup.find('tbody')
            table = PlayByPlayTable(self.id, table_soup, 'regular_season')
            for record in table.get_records():
                if not session.query(PlayByPlayRecordRegularSeason).filter(PlayByPlayRecordRegularSeason.id==record.id, PlayByPlayRecordRegularSeason._Season==record._Season, PlayByPlayRecordRegularSeason._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()

        # playoffs
        pbp_soup = soup.find('div', id='div_playoffs_pbp')
        if pbp_soup:
            table_soup = pbp_soup.find('tbody')
            table = PlayByPlayTable(self.id, table_soup, 'playoffs')
            for record in table.get_records():
                if not session.query(PlayByPlayRecordPlayoffs).filter(PlayByPlayRecordPlayoffs.id==record.id, PlayByPlayRecordPlayoffs._Season==record._Season, PlayByPlayRecordPlayoffs._Tm==record._Tm).all():
                    session.add(record)
                    session.commit()




class PerGameTable():
    table: Any
    id: int
    season_type: str

    def __init__(self, id, soup, season_type):
        self.id = id
        self.table = soup
        self.season_type = season_type
        _keys17 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST', '_PF', '_PTS']
        _keys21 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_PF', '_PTS']
        _keys22 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
        _keys27 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_TOV', '_PF', '_PTS']
        _keys28 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_BLK', '_TOV', '_PF', '_PTS']
        _keys29 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
        self._keys_dict = {17: _keys17, 21: _keys21, 22: _keys22, 27: _keys27, 28: _keys28, 29: _keys29}
        
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
            # if not _tr.find_all('td'):
            #     continue
            _td_list = [_.text for _ in _tr.find_all('td')]
            if not _td_list:
                continue
            header = self._keys_dict[len(_td_list)]
            stats = {k: v for k, v in zip(header, _td_list)}
            stats['_Season'] = _Season
            stats['id'] = self.id
            # スタッツが空白対策
            del_target_set = set()
            for k, v in stats.items():
                if v != 0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del stats[k]
            
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
        _keys17 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST', '_PF', '_PTS']
        _keys19 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST', '_PF', '_PTS', '_tmp', '_TrpDbl']
        _keys21 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_PF', '_PTS']
        _keys22 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
        _keys23 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_PF', '_PTS', '_tmp', '_TrpDbl']
        _keys24 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_TrpDbl']
        _keys27 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_TOV', '_PF', '_PTS']
        _keys28 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_BLK', '_TOV', '_PF', '_PTS']
        _keys29 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
        _keys30 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_TrpDbl']
        _keys31 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_TrpDbl']
        self._keys_dict = {17: _keys17, 19: _keys19, 21: _keys21, 22: _keys22, 23: _keys23, 24: _keys24, 27: _keys27, 28: _keys28, 29: _keys29, 30: _keys30, 31: _keys31}

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
            header = self._keys_dict[len(_td_list)]
            stats = {k: v for k, v in zip(header, _td_list)}
            stats['id'] = self.id
            stats['_Season'] = _Season
            # スタッツが空対策
            del_target_set = set()
            for k, v in stats.items():
                if v != 0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del stats[k]

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
            yield totals_record


class PerGameRecordRegularSeason(Base):
    __tablename__ = 'each_player_overview__per_game__regular_season'
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
    __tablename__ = 'each_player_overview__per_game__playoffs'
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


class TotalsRecordRegularSeason(Base):
    __tablename__ = 'each_player_overview__totals__regular_season'
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
    __tablename__ = 'each_player_overview__totals__playoffs'
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

@dataclass
class Per36MinutesTable():
    id: int
    table_soup: Any
    season_type: str
    # _keys = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
    _keys = dict()
    _keys17 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST', '_PF', '_PTS']
    _keys21 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_PF', '_PTS']
    _keys22 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
    _keys26 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_TOV', '_PF', '_PTS']
    _keys27 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_BLK', '_TOV', '_PF', '_PTS']
    _keys28 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']
    _keys[17] = _keys17
    _keys[21] = _keys21
    _keys[22] = _keys22
    _keys[26] = _keys26
    _keys[27] = _keys27
    _keys[28] = _keys28

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            # th 
            if not _tr.find('th'):
                continue
            # statsがあってもSeasonの欄がhrefでない場合対策
            if _tr.find('th').find('a'):
                _Season = _tr.find('th').find('a').text
            else: 
                _Season = _tr.find('th').text
            # td list
            td_list = [_.text for _ in _tr.find_all('td')]
            if not td_list:
                continue
            header = self._keys[len(td_list)]
            info = {k: v for k, v in zip(header, td_list)}
            info['_Season'] = _Season
            info['id'] = self.id
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            if self.season_type == 'regular_season':
                record = Per36MinutesRecordRegularSeason(**info)
            else:
                record = Per36MinutesRecordPlayoffs(**info)
            yield record
    
class Per36MinutesRecordRegularSeason(Base):
    __tablename__ = f'each_player_overview__per_36_minutes__regular_season'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Integer)
    _FG = Column(Float)
    _FGA = Column(Float)
    _FG_percent = Column(Float)
    _3P = Column(Float)
    _3PA = Column(Float)
    _3P_percent = Column(Float)
    _2P = Column(Float)
    _2PA = Column(Float)
    _2P_percent = Column(Float)
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

class Per36MinutesRecordPlayoffs(Base):
    __tablename__ = f'each_player_overview__per_36_minutes__playoffs'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Integer)
    _FG = Column(Float)
    _FGA = Column(Float)
    _FG_percent = Column(Float)
    _3P = Column(Float)
    _3PA = Column(Float)
    _3P_percent = Column(Float)
    _2P = Column(Float)
    _2PA = Column(Float)
    _2P_percent = Column(Float)
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


@dataclass
class Per100PossTable:
    id: int
    table_soup: Any
    season_type: str
    # _keys   = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_ORtg', '_DRtg']
    _keys24 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_PF', '_PTS', '_tmp', '_ORtg', '_DRtg']
    _keys25 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_ORtg', '_DRtg']
    _keys31 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_ORtg', '_DRtg']
    _keys = dict()
    _keys[24] = _keys24
    _keys[25] = _keys25
    _keys[31] = _keys31

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            # th 
            if not _tr.find('th'):
                continue
            # statsがあってもSeasonの欄がhrefでない場合対策
            if _tr.find('th').find('a'):
                _Season = _tr.find('th').find('a').text
            else: 
                _Season = _tr.find('th').text
            # td list
            td_list = [_.text for _ in _tr.find_all('td')]
            if not td_list:
                continue
            header = self._keys[len(td_list)]
            info = {k: v for k, v in zip(header, td_list)}
            info['_Season'] = _Season
            info['id'] = self.id
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            if self.season_type == 'regular_season':
                record = Per100PossRecordRegularSeason(**info)
            else:
                record = Per100PossRecordPlayoffs(**info)
            yield record


class Per100PossRecordRegularSeason(Base):
    __tablename__ = f'each_player_overview__per_100_poss__regular_season'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Integer)
    _FG = Column(Float)
    _FGA = Column(Float)
    _FG_percent = Column(Float)
    _3P = Column(Float)
    _3PA = Column(Float)
    _3P_percent = Column(Float)
    _2P = Column(Float)
    _2PA = Column(Float)
    _2P_percent = Column(Float)
    _FT = Column(Float)
    _FTA = Column(Float)
    _FT_percent = Column(Float)
    _ORB = Column(Float)
    _DRB = Column(Float)
    _TRB = Column(Float)
    _AST = Column(Float)
    _STL = Column(Float)
    _BLK =Column(Float)
    _TOV = Column(Float)
    _PF = Column(Float)
    _PTS = Column(Float)
    _ORtg = Column(Integer)
    _DRtg = Column(Integer)

class Per100PossRecordPlayoffs(Base):
    __tablename__ = f'each_player_overview__per_100_poss__playoffs'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _GS = Column(Integer)
    _MP = Column(Integer)
    _FG = Column(Float)
    _FGA = Column(Float)
    _FG_percent = Column(Float)
    _3P = Column(Float)
    _3PA = Column(Float)
    _3P_percent = Column(Float)
    _2P = Column(Float)
    _2PA = Column(Float)
    _2P_percent = Column(Float)
    _FT = Column(Float)
    _FTA = Column(Float)
    _FT_percent = Column(Float)
    _ORB = Column(Float)
    _DRB = Column(Float)
    _TRB = Column(Float)
    _AST = Column(Float)
    _STL = Column(Float)
    _BLK =Column(Float)
    _TOV = Column(Float)
    _PF = Column(Float)
    _PTS = Column(Float)
    _ORtg = Column(Integer)
    _DRtg = Column(Integer)


@dataclass
class AdvancedTable:
    id: int
    table_soup: Any
    season_type: str
    # _keys = ['_Player', '_Age', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '__tmp', '_OBPM', '_DBPM', '_BPM', '_VORP']
    _keys18 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48']
    _keys19 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '_tmp']
    _keys21 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48']
    _ky22_1 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '__tmp']
    _ky22_2 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48']
    _ky23_1 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '__tmp']
    _ky23_2 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48']
    _keys24 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '__tmp']
    _keys25 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '_tmptmp', '_OBPM', '_DBPM', '_BPM', '_VORP']
    _keys27 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '_tmptmp', '_OBPM', '_DBPM', '_BPM', '_VORP']
    _keys28 = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '_tmptmp', '_OBPM', '_DBPM', '_BPM', '_VORP']
    _keys = {18: _keys18, 19: _keys19, 21: _keys21, '22_1': _ky22_1, '22_2':_ky22_2 ,'23_1': _ky23_1, '23_2': _ky23_2, 24: _keys24, 25: _keys25, 27: _keys27, 28: _keys28}

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            # th 
            if not _tr.find('th'):
                continue
            # statsがあってもSeasonの欄がhrefでない場合対策
            if _tr.find('th').find('a'):
                _Season = _tr.find('th').find('a').text
            else: 
                _Season = _tr.find('th').text
            # td list
            td_list = [_.text for _ in _tr.find_all('td')]
            if not td_list:
                continue
            if len(td_list) == 22 or len(td_list) == 23:
                if td_list[-1] != 0. and not td_list[-1] and td_list[-6]!=0. and not td_list[-6]:
                    header = self._keys[f'{len(td_list)}_1']
                else:
                    header = self._keys[f'{len(td_list)}_2']
            else:
                header = self._keys[len(td_list)]
            info = {k: v for k, v in zip(header, td_list)}
            print("======"*30)
            print(info)
            info['_Season'] = _Season
            info['id'] = self.id
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            
            if self.season_type == 'regular_season':
                record = AdvancedRecordRegularSeason(**info)
            else:
                record = AdvancedRecordPlayoffs(**info)
            yield record



class AdvancedRecordRegularSeason(Base):
    __tablename__ = f'each_player_overview__advaned__regular_season'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _MP = Column(Integer)
    _PER = Column(Float) 
    _TS_percent = Column(Float) 
    _3PAr = Column(Float) 
    _FTr = Column(Float) 
    _ORB_percent = Column(Float) 
    _DRB_percent = Column(Float) 
    _TRB_percent = Column(Float) 
    _AST_percent = Column(Float) 
    _STL_percent = Column(Float) 
    _BLK_percent = Column(Float) 
    _TOV_percent = Column(Float) 
    _USG_percent = Column(Float) 
    _OWS = Column(Float)
    _DWS = Column(Float) 
    _WS = Column(Float) 
    _WS_48 = Column(Float) 
    _OBPM = Column(Float) 
    _DBPM = Column(Float) 
    _BPM = Column(Float) 
    _VORP = Column(Float) 


class AdvancedRecordPlayoffs(Base):
    __tablename__ = f'each_player_overview__advaned__playoffs'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _MP = Column(Integer)
    _PER = Column(Float) 
    _TS_percent = Column(Float) 
    _3PAr = Column(Float) 
    _FTr = Column(Float) 
    _ORB_percent = Column(Float) 
    _DRB_percent = Column(Float) 
    _TRB_percent = Column(Float) 
    _AST_percent = Column(Float) 
    _STL_percent = Column(Float) 
    _BLK_percent = Column(Float) 
    _TOV_percent = Column(Float) 
    _USG_percent = Column(Float) 
    _OWS = Column(Float)
    _DWS = Column(Float) 
    _WS = Column(Float) 
    _WS_48 = Column(Float) 
    _OBPM = Column(Float) 
    _DBPM = Column(Float) 
    _BPM = Column(Float) 
    _VORP = Column(Float) 


@dataclass
class PlayByPlayTable:
    id: int
    table_soup: Any
    season_type: str
    _keys = ['_Age', '_Tm', '_Lg', '_Pos', '_G', '_MP', '_PG_percent', '_SG_percent', '_SF_percent', '_PF_percent', '_C_percent', '_OnCourt', '_On_Off', '_BadPass', '_LostBall', '_Fouls_Committed__Shoot', '_Fouls_Committed__Off', '_Fouls_Drawn__Shoot', '_Fouls_Drawn__Off', '_PGA', '_And1', '_Blkd']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            # th 
            if not _tr.find('th'):
                continue
            # statsがあってもSeasonの欄がhrefでない場合対策
            if _tr.find('th').find('a'):
                _Season = _tr.find('th').find('a').text
            else: 
                _Season = _tr.find('th').text
            # td list
            td_list = [_.text.replace('%','') for _ in _tr.find_all('td')]
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = _Season
            info['id'] = self.id
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            if self.season_type == 'regular_season':
                record = PlayByPlayRecordRegularSeason(**info)
            else:
                record = PlayByPlayRecordPlayoffs(**info)
            yield record

class PlayByPlayRecordRegularSeason(Base):
    __tablename__ = f'each_player_overview__pbp__regular_season'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _MP = Column(Integer)
    _PG_percent = Column(Integer)
    _SG_percent = Column(Integer)
    _SF_percent = Column(Integer)
    _PF_percent = Column(Integer)
    _C_percent = Column(Integer)
    _OnCourt = Column(Float)
    _On_Off = Column(Float)
    _BadPass = Column(Integer)
    _LostBall = Column(Integer)
    _Fouls_Committed__Shoot = Column(Integer)
    _Fouls_Committed__Off = Column(Integer)
    _Fouls_Drawn__Shoot = Column(Integer)
    _Fouls_Drawn__Off = Column(Integer)
    _PGA = Column(Integer)
    _And1 = Column(Integer)
    _Blkd = Column(Integer)


class PlayByPlayRecordPlayoffs(Base):
    __tablename__ = f'each_player_overview__pbp__playoffs'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _Age = Column(Integer)
    _Tm = Column(String(16), primary_key=True)
    _Lg = Column(String(16))
    _Pos = Column(String(16))
    _G = Column(Integer)
    _MP = Column(Integer)
    _PG_percent = Column(Integer)
    _SG_percent = Column(Integer)
    _SF_percent = Column(Integer)
    _PF_percent = Column(Integer)
    _C_percent = Column(Integer)
    _OnCourt = Column(Float)
    _On_Off = Column(Float)
    _BadPass = Column(Integer)
    _LostBall = Column(Integer)
    _Fouls_Committed__Shoot = Column(Integer)
    _Fouls_Committed__Off = Column(Integer)
    _Fouls_Drawn__Shoot = Column(Integer)
    _Fouls_Drawn__Off = Column(Integer)
    _PGA = Column(Integer)
    _And1 = Column(Integer)
    _Blkd = Column(Integer)




if __name__ == '__main__':
    Base.metadata.create_all(bind=ENGINE)
    session_NBA3 = create_session('NBA3')
    for index, _player, player_overview_url in session_NBA3.query(AllPlayersRecord.id, AllPlayersRecord._player, AllPlayersRecord._url).all():
        
        # # ここのコメントアウトを戻します
        # is_active_player = None
        # for _ in session.execute(f'SELECT * FROM NBA_teams.team_stats__roster WHERE _Player="{_player}"'):
        #     is_active_player = _
        # if not is_active_player:
        #     continue

        # # if 2779 <= index < 3441:
        # #     continue


        # # 1人のプレイヤーを示す
        # overview_page = EachPlayerOverViewPage(index, player_overview_url)
        # # overview_page.create_tables('per_games')
        # # overview_page.create_tables('totals')
        # overview_page.update_each_player_tables()

        a = list(session_NBA3.execute(f'SELECT * FROM NBA3.all_players WHERE _player="{_player}"'))
        # if not a:
        print(a)