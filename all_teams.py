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
DB_NAME = 'NBA4'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

OPTION = 2021 # 2020-21 season

TEAMS_SET = get_teams_set()

# @dataclass
# class Team:
#     team_name: str
#     abbreviation: str
#     coference: str

def insert_with_update_at(tmp_class):
    ts = now 
    tmp_class['update_at'] = ts
    session.add(tmp_class)
    session.commit()

def insert_with_created_at(tmp_class):
    ts = now 
    tmp_class['created_at'] = ts
    tmp_class['updated_at'] = ts
    session.add(tmp_class)
    session.commit()



class TeamPage():
    season: int
    url: str
    team: Team

    def __init__(self, team: Team, season: str):
        self.season = season
        self.team = team
        self.url = f'https://www.basketball-reference.com/teams/{self.team}/{self.season}.html'

    def create_table(self):
        Base.metadata.create_all(bind=ENGINE)

    def update_latest_records(self):
        teams_set = get_teams_set()
        soup = get_soup_by_url(self.url, True)

        # team info
        team_logo_url = soup.find('img', class_ = 'teamlogo').get('src')
        team_info_record = TeamInfoRecord(self.team.season, self.team.abbreviation, self.team.conference, team_logo_url)

        # roster record 
        roster_table_soup = soup.find('table', id = 'roster').find('tbody')
        roster_table = RosterTable(roster_table_soup, self.season, self.team)
        for record: RosterRecord in roster_table.get_records():
            if not session.query(RosterRecord).filter(RosterRecord._Season==record._Season, RosterRecord._abbreviation==record._abbreviation, RosterRecord._Player==record._Player):
                session.add(record)
                session.commit()
                
        # injury report
        injury_report_soup = soup.find('div', id='div_injuries').find('tbody')
        injury_report_table = InjuryReportTable(injury_report_soup, self.season, self.team)
        for injury_record in injury_report_table.get_records():
            if injury_record not in hogehoge:
                insert injury_record

        # Team and Opponent Stats
        team_and_opponent_stats_soup_list = soup.find('div', id='div_team_and_opponent').find_all('tbody')
        team_stats_soup = team_and_opponent_stats_soup_list[0]
        opponent_stats_soup = team_and_opponent_stats_soup_list[1]
        team_stats_table = TeamAndOpponentStatsTable(team_stats_soup, self.season, self.team)
        team_record, team_g_record, lg_rank_record, year_year_record = team_stats_table.get_records()
        if team_record not in hogehoge:
            insert team_record
        

        opponent_stats_table = TeamAndOpponentStatsTable(opponent_stats_soup, self.season, self.team)
        team_record, team_g_record, lg_rank_record, year_year_record = opponent_stats_table.get_records()



        # Team Misc
        team_misc_soup = soup.find('div', id='div_team_misc').find('tbody')
        team_misc_table = TeamMiscTable(team_misc_soup)

        # PerGame
        per_game_soup = soup.find('div', id='all_per_game').find('tbody')
        per_game_table = PerGameTable(per_game_soup)

        # Totals
        totals_soup = soup.find('div', id='div_totals').find('tbody')
        totals_table = TotalsTable(totals_soup)

        # Per 36 Minutes
        per_36_minutes_soup = soup.find('div', id='div_per_minutes').find('tbody')

        # Per 100 poss
        per_100_poss_soup = soup.find('div', id='div_per_poss').find('tbody')

        # Advanced
        advanced_soup = soup.find('div', id='div_advanced').find('tbody')

        # Play by Play
        play_by_play_soup = soup.find('div', id='div_pbp').find('tbody')

# 基本情報
class TeamInfoRecord(Base):
    __tablename__ = f'team_stats'
    _name = Column(String(120))
    _abbreviation = Column(String(120), primary_key=True)
    _conference = Column(String(4))
    _logo_url = Column(String(120))

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ロスター情報
class RosterTable:
    """ webページに書いてある表を表すクラス
        メソッドは表の行を返す
    """
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Pos', '_Ht', '_Wt', '_BirthDate', '_CountryCode', '_Exp', '_College']
    
    def __init__(self, soup):
        self.table_soup = soup

    def get_records(self):
        for  _tr in self.table_soup.find_all('tr'):
            _No = _tr.find('th').text
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k:v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_No'] = _No
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = RosterRecord(info.__dict__.items())
            yield record

class RosterRecord(Base):
    __tablename__ = f'team_stats_members_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _No = Column(String(16)) # 2つ以上背番号を持つ選手用に
    _Player = Column(String(120), primary_key=True)
    _Pos = Column(String(16))
    _Ht = Column(String(16))
    _Wt = Column(Integer)
    _BirthDate = Column(String(120)) # Date型にしたい
    _CountryCode = Column(String(16)) # usなど
    _Experience = Column(Integer) # Rは謎
    _College = Column(String(120))
    _created_at = Column(String(120))

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Injury 情報
class InjuryReportTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Team', '_Update', '_Description']

    def __init__(self, table_soup, season, team):
        self.table_soup = table_soup
        self.season = season
        self.team = team

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            _Player = _tr.find('th').find('a').text
            td_list = [_ for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_Player'] = _Player
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = InjuryReportRecord(info.__dict__.items())
            yield record

class InjuryReportRecord(Base):
    __tablename__ = f'team_stats_injury_reports_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120))
    _Team = Column(String(120))
    _Update = Column(String(120))
    _Description = Column(String(120))

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Team and Opponent Stats
@dataclass
class TeamAndOpponentStatsTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_G', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']    

    def get_records(self):
        def _get_info(td_list, year_year_flag=False):
            info = {k: v for k, v in zip(self.team_and_opponent_stats_keys, td_list)}
            if year_year_flag:
                info = {k: v.replace('%', '') for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            return info

        tr_list = self.table_soup.find_all('tr')
        Team_row, Team_G_row, Lg_Rank_row, Year_Year_row = tr_list
        # Team
        info = _get_info([_ for _ in Team_row.find_all('td')])
        team_record = TeamAndOpponentStatsTeamRecord(info.__dict__.items())
        # Team/G
        info = _get_info([_ for _ in Team_G_row.find_all('td')])
        team_g_record = TeamAndOpponentStatsLgRankRecord(info.__dict__.items())
        # Lg Rank
        info = _get_info([_ for _ in Lg_Rank_row.find_all('td')])
        lg_rank_record = TeamAndOpponentStatsYearYearRecord(info.__dict__.items())
        # Year/Year
        info = _get_info([_ for _ in Year_Year_row.find_all('td')])
        year_year_record = TeamAndOpponentStatsYearYearRecord(info.__dict__.items())
        # 各チーム1行ずつのみなので yield ではなく return を使う
        return team_record, team_g_record, lg_rank_record, year_year_record
        
class TeamAndOpponentStatsTeamRecord(Base):
    __tablename__ = f'team_stats_team_and_opponent_stats__Team__{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _G = Column(Integer)
    _MP  = Column(Integer)
    _FG  = Column(Integer)
    _FGA = Column(Integer)
    _FG_percent  = Column(Float)
    _3P = Column(Integer)
    _3PA = Column(Integer)
    _3P_percent = Column(Float)
    _2P = Column(Integer)
    _2PA = Column(Integer)
    _2P_percent = Column(Float)
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
    _created_at = Column(String(120), primary_key=True)

class TeamAndOpponentStatsTeamGRecord(Base):
    __tablename__ = f'team_stats_team_and_opponent_stats__Team_G__{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _G = Column(Integer)
    _MP  = Column(Float)
    _FG  = Column(Float)
    _FGA = Column(Float)
    _FG_percent  = Column(Float)
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
    _created_at = Column(String(120), primary_key=True)

class TeamAndOpponentStatsLgRankRecord(Base):
    __tablename__ = f'team_stats_team_and_opponent_stats__Lg_Rank__{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _G = Column(Integer)
    _MP  = Column(Integer)
    _FG  = Column(Integer)
    _FGA = Column(Integer)
    _FG_percent  = Column(Integer)
    _3P = Column(Integer)
    _3PA = Column(Integer)
    _3P_percent = Column(Integer)
    _2P = Column(Integer)
    _2PA = Column(Integer)
    _2P_percent = Column(Integer)
    _FT = Column(Integer)
    _FTA = Column(Integer)
    _FT_percent = Column(Integer)
    _ORB = Column(Integer)
    _DRB = Column(Integer)
    _TRB = Column(Integer)
    _AST = Column(Integer)
    _STL = Column(Integer)
    _BLK = Column(Integer)
    _TOV = Column(Integer)
    _PF = Column(Integer)
    _PTS = Column(Integer)
    _created_at = Column(String(120), primary_key=True)

class TeamAndOpponentStatsYearYearRecord(Base):
    __tablename__ = f'team_stats_team_and_opponent_stats__Year_Year__{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _G = Column(Float)
    _MP  = Column(Float)
    _FG  = Column(Float)
    _FGA = Column(Float)
    _FG_percent  = Column(Float)
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
    _created_at = Column(String(120), primary_key=True)
    
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class TeamMisc:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_W', '_L', '_PW', '_PL', '_MOV', '_SOS', '_SRS', '_ORtg', '_DRtg', '_Pace', '_Advanced__FTr', '_Advanced__3PAr', '_Offence_Four_Factor__eFG_percent', '_Offence_Four_Factor__TOV_percent', '_Offence_Four_Factor__ORB_percent', '_Offence_Four_Factor__FT_FGA', '_Defense_Four_Factor__eFG_percent', '_Defense_Four_Factor__TOV_percent', '_Defense_Four_Factor__DRB_percent', '_Defense_Four_Factor__FT_FGA', '_Arena', '_Attendance']

    def get_records(self):
        def _get_info(td_list):
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            return info
        
        tr_list = self.table_soup.find_all('tr')
        team_row, lg_rank_row = tr_list
        info = _get_info([_.text for _ in team_row.find_all('td')])
        team_misc_team_record = TeamMiscTeamRecord(info.__dict__.items())
        
        info = _get_info([_.text for _ in lg_rank_row.find_all('td')])
        team_misc_lg_rank_record = TeamMiscLgRankRecord(info.__dict__.items())

        return team_misc_team_record, team_misc_lg_rank_record

class TeamMiscTeamRecord(Base):
    __tablename__ = f'team_stats_team_misc_team_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _W = Column(Integer)
    _L = Column(Integer)
    _PW = Column(Integer)
    _PL = Column(Integer)
    _MOV = Column(Float)
    _SOS = Column(Float)
    _SRS = Column(Float)
    _ORtg = Column(Float)
    _DRtg = Column(Float)
    _Pace = Column(Float)
    _Advanced__FTr = Column(Float)
    _Advanced__3PAr = Column(Float)
    _Offence_Four_Factor__eFG_percent = Column(Float)
    _Offence_Four_Factor__TOV_percent = Column(Float)
    _Offence_Four_Factor__ORB_percent = Column(Float)
    _Offence_Four_Factor__FT_FGA = Column(Float)
    _Defense_Four_Factor__eFG_percent = Column(Float)
    _Defense_Four_Factor__TOV_percent = Column(Float)
    _Defense_Four_Factor__DRB_percent = Column(Float)
    _Defense_Four_Factor__FT_FGA = Column(Float)
    _Arena = Column(String(120))
    _Attendance = Column(String(120))
    _created_at = Column(String(120), primary_key=True)


class TeamMiscLgRankRecord(Base):
    __tablename__ = f'team_stats_team_misc_lg_rank_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _W = Column(Integer)
    _L = Column(Integer)
    _PW = Column(Integer)
    _PL = Column(Integer)
    _MOV = Column(Integer)
    _SOS = Column(Integer)
    _SRS = Column(Integer)
    _ORtg = Column(Integer)
    _DRtg = Column(Integer)
    _Pace = Column(Integer)
    _Advanced__FTr = Column(Integer)
    _Advanced__3PAr = Column(Integer)
    _Offence_Four_Factor__eFG_percent = Column(Integer)
    _Offence_Four_Factor__TOV_percent = Column(Integer)
    _Offence_Four_Factor__ORB_percent = Column(Integer)
    _Offence_Four_Factor__FT_FGA = Column(Integer)
    _Defense_Four_Factor__eFG_percent = Column(Integer)
    _Defense_Four_Factor__TOV_percent = Column(Integer)
    _Defense_Four_Factor__DRB_percent = Column(Integer)
    _Defense_Four_Factor__FT_FGA = Column(Integer)
    _Arena = Column(String(120))
    _Attendance = Column(String(120))
    _created_at = Column(String(120), primary_key=True)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class PerGameTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS_G']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = PerGameRecord(info.__dict__.items())
            yield record

# _Rk は関係ない
class PerGameRecord(Base):
    __tablename__ = f'team_stats_per_game_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120))
    _Age = Column(Integer)
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
    _PTS_G = Column(Float)
    _created_at = Column(String(120), primary_key=True)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class TotalsTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = TotalsRecord(info.__dict__.items())
            yield record

class TotalsRecord(Base):
    __tablename__ = f'team_stats_totals_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120))
    _Age = Column(Integer)
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
    _created_at = Column(String(120), primary_key=True)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class Per36MinutesTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = Per36MinutesRecord(info.__dict__.items())
            yield record

class Per36MinutesRecord(Base):
    __tablename__ = f'team_stats_per36_minutes_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120))
    _Age = Column(Integer)
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
    _created_at = Column(String(120), primary_key=True)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class Per100PossTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_eFG_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_tmp', '_ORtg', '_DRtg']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            del info['_tmp']
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = Per100PossRecord(info.__dict__.items())
            yield record

class Per100PossRecord(Base):
    __tablename__ = f'team_stats_per100_poss_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120))
    _Age = Column(Integer)
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
    _ORtg = Column(Integer)
    _DRtg = Column(Integer)
    _created_at = Column(String(120), primary_key=True)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class AdvancedTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_MP', '_PER', '_TS_percent', '_3PAr', '_FTr', '_ORB_percent', '_DRB_percent', '_TRB_percent', '_AST_percent', '_STL_percent', '_BLK_percent', '_TOV_percent', '_USG_percent', '_tmp', '_OWS', '_DWS', '_WS', '_WS_48', '__tmp', '_OBPM', '_DBPM', '_BPM', '_VORP']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            del info['_tmp']
            del info['__tmp']
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = AdvancedRecord(info.__dict__.items())
            yield record

class AdvancedRecord(Base):
    __tablename__ = f'team_stats_advanced_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120))
    _Age = Column(Integer)
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
    _created_at = Column(String(120), primary_key=True)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class PlayByPlayTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['__PositionEstimate__Player', '__PositionEstimate__Age', '__PositionEstimate__G', '__PositionEstimate__MP', '__PositionEstimate__PG_percent', '__PositionEstimate__SG_percent', '__PositionEstimate__SF_percent', '__PositionEstimate__PF_percent', '__PositionEstimate__C_percent','__PlusMinus_Per_100_Poss__OnCourt', '__PlusMinus_Per_100_Poss__OnOff', '__Turnovers__BadPass', '__Turnovers__LostBall', '__Fouls_Committed__Shoot', '__Fouls_Committed__Off', '__Fouls_Drawn__Shoot', '__Fouls_Drawn__Off', '__Misc__PGA', '__Misc__And1', '__Misc__Blkd']
    position_rate_keys = {'__PositionEstimate__PG_percent', '__PositionEstimate__SG_percent', '__PositionEstimate__SF_percent', '__PositionEstimate__PF_percent', '__PositionEstimate__C_percent'}
    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v if k  not in for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = time.strtime('%a-%b-%d')
            record = AdvancedRecord(info.__dict__.items())
            yield record
    
class PlayByPlayRecord(Base):
    __tablename__ = f'team_stats_play_by_play_{OPTION}'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    __PositionEstimate__Player = Column(String(120))
    __PositionEstimate__Age = Column(Integer)
    __PositionEstimate__G = Column(Integer)
    __PositionEstimate__MP = Column(Integer)
    __PositionEstimate__PG_percent = Column(Integer)
    __PositionEstimate__SG_percent = Column(Integer)
    __PositionEstimate__SF_percent = Column(Integer)
    __PositionEstimate__PF_percent =Column(Integer)
    __PositionEstimate__C_percent = Column(Integer)
    __PlusMinus_Per_100_Poss__OnCourt = Column(Float) 
    __PlusMinus_Per_100_Poss__OnOff = Column(Float)
    __Turnovers__BadPass = Column(Integer)
    __Turnovers__LostBall = Column(Integer)
    __Fouls_Committed__Shoot = Column(Integer)
    __Fouls_Committed__Off = Column(Integer)
    __Fouls_Drawn__Shoot = Column(Integer)
    __Fouls_Drawn__Off = Column(Integer)
    __Misc__PGA = Column(Integer)
    __Misc__And1 = Column(Integer) 
    __Misc__Blkd = Column(Integer)
    _created_at = Column(String(120), primary_key=True)


if __name__ == '__main__':
    for team in TEAMS_SET:
        member_page = MemberPage(team, OPTION)




    
    for index, _player, player_overview_url in session.query(AllPlayersRecord.id, AllPlayersRecord._player, AllPlayersRecord._url).all():
        # tm が異なってもGameLogには1シーズン情報で出てくるので.first()でOK
        for res in session.query(distinct(PerGameRecord._Season)).filter(PerGameRecord.id == index).all():
            _Season = res[0]
