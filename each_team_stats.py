import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Text, DateTime, Sequence, ForeignKey, create_engine, MetaData, DECIMAL, DATETIME, exc, event, Index, distinct
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.ext.declarative import declarative_base

import requests
from bs4 import BeautifulSoup
from pathlib import Path
from utils import ALPHABET, HOST, DB_NAME
from utils import get_mysql_pass, get_soup_by_url, get_teams_set
from typing import Any, List

from all_players import AllPlayersRecord
from each_player_overview import PerGameRecord
import time
from datetime import datetime

from utils import Team

keys_dict = {23: ['_Team', '_G', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']}

Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

OPTION = 2021 # 2020-21 season



class ConferenceStandingsPage():
    season: int
    url: str

    def __init__(self, season):
        self.season = season
        self.url = f'https://www.basketball-reference.com/leagues/NBA_{self.season}.html'
    
    def create_table(self):
        Base.metadata.create_all(bind=ENGINE, tables=[TeamRecord.__table__])

    def insert_latest_records(self):
        teams_set = get_teams_set()
        soup = get_soup_by_url(self.url, True)
        east_conf, west_conf = soup

class TeamRecord(Base):
    __tablename__ = f'all_teams_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _Conference = Column(String(4))
    _Name = Column(String(120))
    _Abbreviation Column(String(16), primary_key=True)
    _W = Column(Integer)
    _L = Column(Integer)
    _WL_percent = Column(Float)
    _GB = Column(Float) # 各カンファレンス1位は0.0とする
    _PS_G = Column(Float)
    _PA_G = Column(Float)
    _SRS = Column(Float)
    _created_at = Column(String(120), primary_key=True)

class TeamPerGameStatsRecord(Base):
    __tablename__ = f'team_stats_team_per_game_stats_{OPTION}'

class OpponentPerGameStatsRecord(Base):
    __tablename__ = f'team_stats_opponent_per_game_stats_{OPTION}'

class TeamStatsRecord(Base):
    __tablename__ = f'team_stats_team_stats_{OPTION}'

class OpponentStatsRecord(Base):
    __tablename__ = f'team_stats_opponent_stats_{OPTION}'

class TeamPer100PossStats(Base):
    __tablename__ = f'team_stats_per_100_poss_stats_{OPTION}'

