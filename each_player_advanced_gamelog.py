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
OPTION = 'all_times' # 2020-21
CURRENT_SEASON = '2020-21'


keys_dict = {
            16: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_ORtg', '_DRtg'],
            18: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_ORtg', '_DRtg'],
            19: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_eFG_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_TOV_percent', '_USG_percent', '_ORtg', '_DRtg'],
            20: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_ORtg', '_DRtg'],
            21: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_ORtg', '_DRtg', '_GmSc'],
            22: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_eFG_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_ORtg', '_DRtg', '_GmSc'],
            23: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL', '_GS', '_MP', '_TS_percent', '_eFG_percent', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_ORtg', '_DRtg', '_GmSc', '_BPM'], 
            8: ['_G', '_Date', '_Age', '_Tm', '_at', '_Opp', '_WL']}
# 19はあとから足したので怪しいかも


class EachPlayerAdvancedGameLogPage():
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
            Base.metadata.create_all(bind=ENGINE, tables=[RegularSeasonAdvancedRecord.__table__])
        if table_name == 'playoffs':
            Base.metadata.create_all(bind=ENGINE, tables=[PlayoffsAdvancedRecord.__table__])

    def update_each_player_advanced_gamelog_tables(self):
        # chromiumでスクレイピング
        soup = get_soup_by_url(self.url, True)
        # table_soups = soup.find_all('tbody')
        
        # get tables
        # regular_season_table_soup, playoffs_table_soup = table_soups[7], table_soups[8]

        # regular season
        all_pgl_advanced_soup =  soup.find(id='div_pgl_advanced')
        if all_pgl_advanced_soup:
            regular_season_table_soup = all_pgl_advanced_soup.find('tbody')
            regular_season_table = RegularSeasonTable(self.id, self.season, regular_season_table_soup)
            for regular_season_record in regular_season_table.get_records():
                if not session.query(RegularSeasonAdvancedRecord.id, RegularSeasonAdvancedRecord._Season, RegularSeasonAdvancedRecord._Date).filter(RegularSeasonAdvancedRecord.id == regular_season_record.id, RegularSeasonAdvancedRecord._Season == regular_season_record._Season, RegularSeasonAdvancedRecord._Date == regular_season_record._Date).first():
                    session.add(regular_season_record)
                    session.commit()
                    
        # playoffs
        div_pgl_advanced_playoffs_soup = soup.find(id='div_pgl_advanced_playoffs')
        if div_pgl_advanced_playoffs_soup:
            playoffs_table_soup = div_pgl_advanced_playoffs_soup.find('tbody')
            playoffs_table = PlayoffsTable(self.id, self.season, playoffs_table_soup)
            for playoffs_record in playoffs_table.get_records():
                if not session.query(PlayoffsAdvancedRecord.id, PlayoffsAdvancedRecord._Season, PlayoffsAdvancedRecord._Date).filter(PlayoffsAdvancedRecord.id == playoffs_record.id, PlayoffsAdvancedRecord._Season == playoffs_record._Season, PlayoffsAdvancedRecord._Date == playoffs_record._Date).first():
                    session.add(playoffs_record)
                    session.commit()

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

            regular_season_record = RegularSeasonAdvancedRecord(**stats)
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

            playoffs_record = PlayoffsAdvancedRecord(**stats)
            yield playoffs_record
    


class RegularSeasonAdvancedRecord(Base):
    __tablename__ = f'each_player_gamelog_advanced_regular_season_{OPTION}_union'
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
    _TS_percent = Column(Float)
    _eFG_percent = Column(Float)
    _ORB_percent = Column(Float)
    _DRB_percent = Column(Float)
    _TRB_percent = Column(Float)
    _AST_percent = Column(Float)
    _STL_percent = Column(Float)
    _BLK_percent = Column(Float)
    _TOV_percent = Column(Float)
    _USG_percent = Column(Float)
    _ORtg = Column(Integer)
    _DRtg = Column(Integer)
    _GmSc = Column(Float)
    _BPM = Column(Float)

class PlayoffsAdvancedRecord(Base):
    __tablename__ = f'each_player_gamelog_advanced_playoffs_{OPTION}_union'
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
    _TS_percent = Column(Float)
    _eFG_percent = Column(Float)
    _ORB_percent = Column(Float)
    _DRB_percent = Column(Float)
    _TRB_percent = Column(Float)
    _AST_percent = Column(Float)
    _STL_percent = Column(Float)
    _BLK_percent = Column(Float)
    _TOV_percent = Column(Float)
    _USG_percent = Column(Float)
    _ORtg = Column(Integer)
    _DRtg = Column(Integer)
    _GmSc = Column(Float)
    _BPM = Column(Float)



if __name__ == '__main__':


    for index, _player, player_overview_url in session.query(AllPlayersRecord.id, AllPlayersRecord._player, AllPlayersRecord._url).all():

        # tm が異なってもGameLogには1シーズン情報で出てくるので.first()でOK
        for res in session.query(distinct(PerGameRecordRegularSeason._Season)).filter(PerGameRecordRegularSeason.id == index).all():
            _Season = res[0]
            year = str(int(_Season.split('-')[0]) + 1 )
            game_log_url = player_overview_url.replace('.html', f'/gamelog-advanced/{year}')

            if _Season != CURRENT_SEASON:
                continue

            # 選手で収集
            # if _player != 'Jason Tytum':
            #     continue

            each_player_advanced_game_log_page = EachPlayerAdvancedGameLogPage(index, _Season, game_log_url)
            
            # create table
            # each_player_advanced_game_log_page.create_tables('regular_season')
            # each_player_advanced_game_log_page.create_tables('playoffs')

            # update table
            each_player_advanced_game_log_page.update_each_player_advanced_gamelog_tables()
