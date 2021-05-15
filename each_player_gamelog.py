import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Text, DateTime, Sequence, ForeignKey, create_engine, MetaData, DECIMAL, DATETIME, exc, event, Index, distinct
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
from each_player_overview import PerGameRecordRegularSeason
import time
from datetime import datetime

Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DB_NAME = 'NBA_gamelog'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

OPTION = 'all_times' # 2019-2020

keys_dict = { 28: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_GmSc'],
            27: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS'],
            19: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_TRB', '_AST', '_PF', '_PTS'], 
            23: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_PF', '_PTS'],
            29: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_GmSc', '_PlusMinus'],
            25: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_GmSc'],
            8: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL']}

class EachPlayerGameLogPage():
    id: int
    season: str
    url: str
    regular_season_table: Any
    playoffs_table: Any

    def __init__(self, id, season, url):
        self.id = id
        self.season = season
        self.url = url
        
    def create_tables(self, table_name):
        if table_name == 'regular_season':
            Base.metadata.create_all(bind=ENGINE, tables=[RegularSeasonRecord.__table__])
        if table_name == 'playoffs':
            Base.metadata.create_all(bind=ENGINE, tables=[PlayoffsRecord.__table__])

    def update_each_player_gamelog_tables(self):
        # chromiumでスクレイピング
        soup = get_soup_by_url(self.url, True)
        # table_soups = soup.find_all('tbody')
        
        # get tables
        # regular_season_table_soup, playoffs_table_soup = table_soups[7], table_soups[8]

        # regular season
        all_pgl_basic_soup =  soup.find(id='div_pgl_basic')
        if all_pgl_basic_soup:
            regular_season_table_soup = all_pgl_basic_soup.find('tbody')
            regular_season_table = RegularSeasonTable(self.id, self.season, regular_season_table_soup)
            
            try:
                for regular_season_record in regular_season_table.get_records():
                    if not session.query(RegularSeasonRecord.id, RegularSeasonRecord._Season, RegularSeasonRecord._Date).filter(RegularSeasonRecord.id == regular_season_record.id, RegularSeasonRecord._Season == regular_season_record._Season, RegularSeasonRecord._Date == regular_season_record._Date).first():
                        session.add(regular_season_record)
                        session.flush()
                session.commit()
            except:
                session.rollback()
            
                        
        # playoffs
        div_pgl_basic_playoffs_soup = soup.find(id='div_pgl_basic_playoffs')
        if div_pgl_basic_playoffs_soup:
            playoffs_table_soup = div_pgl_basic_playoffs_soup.find('tbody')
            playoffs_table = PlayoffsTable(self.id, self.season, playoffs_table_soup)
            
            try:
                for playoffs_record in playoffs_table.get_records():
                    if not session.query(PlayoffsRecord.id, PlayoffsRecord._Season, PlayoffsRecord._Date).filter(PlayoffsRecord.id == playoffs_record.id, PlayoffsRecord._Season == playoffs_record._Season, PlayoffsRecord._Date == playoffs_record._Date).first():
                        session.add(playoffs_record)
                        session.flush()
                session.commit()
            except:
                session.rollback()
            
class RegularSeasonTable():
    table: Any
    id: int
    season: str

    def __init__(self, id, season, soup):
        self.id = id
        self.season = season
        self.table = soup

    def get_records(self):
        for _tr in self.table.find_all('tr'):
            if not _tr.find('th'):
                continue
            if _tr.find('th').find('a'):
                _Rk = _tr.find('th').find('a').text
            else:
                _Rk = _tr.find('th').text
            if not _tr.find_all('td'):
                continue
            _td_list = [_.text for _ in _tr.find_all('td')]
                
            stats = {k: v for k, v in zip(keys_dict[len(_td_list)], _td_list)}
            stats['id'] = self.id
            stats['_Season'] = self.season
            # スタッツが空白対策
            del_target_set = set()
            for stats_type, stats_val in stats.items():
                if stats_val != 0. and not stats_val:
                    del_target_set.add(stats_type)
            for del_target in del_target_set:
                del stats[del_target]

            regular_season_record = RegularSeasonRecord(**stats)
            yield regular_season_record

            
class PlayoffsTable():
    table: Any
    id: int
    season: str

    def __init__(self, id, season, soup):
        self.id = id
        self.season = season
        self.table = soup
    
    def get_records(self):
        for _tr in self.table.find_all('tr'):
            if not _tr.find('th'):
                continue
            if _tr.find('th').find('a'):
                _Rk = _tr.find('th').find('a').text
            else:
                _Rk = _tr.find('th').text
            if not _tr.find_all('td'):
                continue
            _td_list = [_.text for _ in _tr.find_all('td')]
            
            stats = {k: v for k, v in zip(keys_dict[len(_td_list)], _td_list)}
            stats['id'] = self.id
            stats['_Season'] = self.season

            # stats['_Date'] = datetime(year=int(stats['_Date'].split('-')[0]), month=int(stats['_Date'].split('-')[1]), day=int(stats['_Date'].split('-')[2]))
            # スタッツが空白対策
            del_target_set = set()
            for stats_type, stats_val in stats.items():
                if stats_val != 0. and not stats_val:
                    del_target_set.add(stats_type)
            for del_target in del_target_set:
                del stats[del_target]

            playoffs_record = PlayoffsRecord(**stats)
            yield playoffs_record
    


class RegularSeasonRecord(Base):
    __tablename__ = f'each_player_gamelog_regular_season_{OPTION}_3'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)
    _G = Column(Integer)
    _Date = Column(String(16), primary_key=True)
    _Age = Column(String(120))
    _Tm = Column(String(3))
    _at = Column(String(1))
    _Opp = Column(String(3))
    _WL = Column(String(8))
    _GS = Column(Integer)
    _MP = Column(String(8))
    _FG = Column(Integer)
    _FGA = Column(Integer)
    _FG_percent = Column(Float)
    _3P = Column(Integer)
    _3PA = Column(Integer)
    _3P_percent = Column(Float)
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
    _GmSc = Column(Float)
    _PlusMinus = Column(Integer)

class PlayoffsRecord(Base):
    __tablename__ = f'each_player_gamelog_playoffs_{OPTION}_3'
    id = Column(Integer, primary_key=True)
    _Season = Column(String(120), primary_key=True)

    _G = Column(Integer)
    _Date = Column(String(16), primary_key=True)
    _Age = Column(String(120))
    _Tm = Column(String(3))
    _at = Column(String(1))
    _Opp = Column(String(3))
    _WL = Column(String(8))
    _GS = Column(Integer)
    _MP = Column(String(8))
    _FG = Column(Integer)
    _FGA = Column(Integer)
    _FG_percent = Column(Float)
    _3P = Column(Integer)
    _3PA = Column(Integer)
    _3P_percent = Column(Float)
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
    _GmSc = Column(Float)
    _PlusMinus = Column(Integer)

if __name__ == '__main__':
    for index, _player, player_overview_url in session.query(AllPlayersRecord.id, AllPlayersRecord._player, AllPlayersRecord._url).all():

        # if index < 3996:#3437:#2506:#1686:#1403:     
        #     continue

        if index != 3996:
            continue
        

        # tm が異なってもGameLogには1シーズン情報で出てくるので.first()でOK
        for res in session.query(distinct(PerGameRecordRegularSeason._Season)).filter(PerGameRecordRegularSeason.id == index).all():
            _Season = res[0]
            year = str(int(_Season.split('-')[0]) + 1 )
            game_log_url = player_overview_url.replace('.html', f'/gamelog/{year}')

            # if int(year) != OPTION: # 2019-20
            #     continue

            # if _player != 'Jason Tytum':
            #     continue

            each_player_game_log_page = EachPlayerGameLogPage(index, _Season, game_log_url)
            
            # create table
            # each_player_game_log_page.create_tables('regular_season')
            # each_player_game_log_page.create_tables('playoffs')

            # update table
            each_player_game_log_page.update_each_player_gamelog_tables()
