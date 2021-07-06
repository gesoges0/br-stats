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
from each_player_overview import PerGameRecordRegularSeason
import time
from dataclasses import dataclass

from datetime import datetime
from utils import Team

keys_dict = {23: ['_Team', '_G', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']}

Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DB_NAME = 'NBA_teams'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

OPTION = 2021 # 2020-21 season
CURRENT_SEASON = 2021
TEAMS_SET = get_teams_set()
NOW = datetime.now()

class TeamPage():
    season: int
    url: str
    team: Team

    def __init__(self, team: Team, season: int):
        self.season = season
        self.team = team
        self.url = f'https://www.basketball-reference.com/teams/{self.team.abbreviation}/{self.season}.html'

    def create_table(self):
        Base.metadata.create_all(bind=ENGINE)

    def update_latest_records(self):
        soup = get_soup_by_url(self.url, True)

        
        
        # # team info
        # # 更新なし
        team_logo_link = soup.find('img', class_ = 'teamlogo').get('src')
        team_info_dict = {'_abbreviation': self.team.abbreviation, '_name': self.team.team_name, '_conference': self.team.coference, '_logo_link': team_logo_link}
        team_info_record = TeamInfoRecord(**team_info_dict)
        if not session.query(TeamInfoRecord).filter(TeamInfoRecord._abbreviation == team_info_record._abbreviation).all():
            session.add(team_info_record)
            session.commit()

        
        # # roster record 
        # # created_atがあるため更新則はこのまま
        # いなくなった選手はPerGameとの差分で分かる
        roster_table_soup = soup.find('table', id = 'roster').find('tbody')
        roster_table = RosterTable(roster_table_soup, self.season, self.team)
        for record in roster_table.get_records():
            # not exists
            if not session.query(RosterRecord).filter(RosterRecord._Season==record._Season, RosterRecord._abbreviation==record._abbreviation, RosterRecord._Player==record._Player).all():
                session.add(record)
                session.commit()


        # injury report
        # created_atがあるため更新則はこのまま
        # injury report が無い場合があるのでtry
        try:
            injury_report_soup = soup.find('div', id='div_injuries').find('tbody')
            injury_report_table = InjuryReportTable(injury_report_soup, self.season, self.team)
            for record in injury_report_table.get_records():
                if not session.query(InjuryReportRecord).filter(InjuryReportRecord._Season==record._Season, InjuryReportRecord._abbreviation==record._abbreviation, InjuryReportRecord._Player==record._Player, InjuryReportRecord._Update==record._Update).all():
                    session.add(record)
                    session.commit()
        except:
            pass
            
        # ==========================================================================================

        # # Team and Opponent Stats
        # # created_atがあるため更新則はこのまま
        team_and_opponent_stats_soup_list = soup.find('div', id='div_team_and_opponent').find_all('tbody')
        # # 上4行（自チーム）
        team_stats_soup = team_and_opponent_stats_soup_list[0]
        team_stats_table = TeamAndOpponentStatsTable(team_stats_soup, self.season, self.team, False)
        team_record, team_g_record, lg_rank_record, year_year_record = team_stats_table.get_records()
        if not session.query(TeamAndOpponentStatsTeamRecord).filter(TeamAndOpponentStatsTeamRecord._Season==team_record._Season, TeamAndOpponentStatsTeamRecord._abbreviation==team_record._abbreviation, TeamAndOpponentStatsTeamRecord._created_at==team_record._created_at).all():
            session.add(team_record)
        if not session.query(TeamAndOpponentStatsTeamGRecord).filter(TeamAndOpponentStatsTeamGRecord._Season==team_g_record._Season, TeamAndOpponentStatsTeamGRecord._abbreviation==team_g_record._abbreviation, TeamAndOpponentStatsTeamGRecord._created_at==team_g_record._created_at).all():
            session.add(team_g_record)
        if not session.query(TeamAndOpponentStatsLgRankRecord).filter(TeamAndOpponentStatsLgRankRecord._Season==lg_rank_record._Season, TeamAndOpponentStatsLgRankRecord._abbreviation==lg_rank_record._abbreviation, TeamAndOpponentStatsLgRankRecord._created_at==lg_rank_record._created_at).all():
            session.add(lg_rank_record)
        if not session.query(TeamAndOpponentStatsYearYearRecord).filter(TeamAndOpponentStatsYearYearRecord._Season==year_year_record._Season, TeamAndOpponentStatsYearYearRecord._abbreviation==year_year_record._abbreviation, TeamAndOpponentStatsYearYearRecord._created_at==year_year_record._created_at).all():
            session.add(year_year_record)
        session.commit()
        
        # # 下4行（自チームに対する相手のレート）
        # # コチラ側はほぼ上のコピペで大丈夫
        opponent_stats_soup = team_and_opponent_stats_soup_list[1]
        opponent_stats_table = TeamAndOpponentStatsTable(opponent_stats_soup, self.season, self.team, True)
        team_record, team_g_record, lg_rank_record, year_year_record = opponent_stats_table.get_records()
        if not session.query(TeamAndOpponentStatsOpponentRecord).filter(TeamAndOpponentStatsOpponentRecord._Season==team_record._Season, TeamAndOpponentStatsOpponentRecord._abbreviation==team_record._abbreviation, TeamAndOpponentStatsOpponentRecord._created_at==team_record._created_at).all():
            session.add(team_record)
        if not session.query(TeamAndOpponentStatsOpponentGRecord).filter(TeamAndOpponentStatsOpponentGRecord._Season==team_g_record._Season, TeamAndOpponentStatsOpponentGRecord._abbreviation==team_g_record._abbreviation, TeamAndOpponentStatsOpponentGRecord._created_at==team_g_record._created_at).all():
            session.add(team_g_record)
        if not session.query(TeamAndOpponentStatsLgRankRecord_Opponent).filter(TeamAndOpponentStatsLgRankRecord_Opponent._Season==lg_rank_record._Season, TeamAndOpponentStatsLgRankRecord_Opponent._abbreviation==lg_rank_record._abbreviation, TeamAndOpponentStatsLgRankRecord_Opponent._created_at==lg_rank_record._created_at).all():
            session.add(lg_rank_record)
        if not session.query(TeamAndOpponentStatsYearYearRecord_Opponent).filter(TeamAndOpponentStatsYearYearRecord_Opponent._Season==year_year_record._Season, TeamAndOpponentStatsYearYearRecord_Opponent._abbreviation==year_year_record._abbreviation, TeamAndOpponentStatsYearYearRecord_Opponent._created_at==year_year_record._created_at).all():
            session.add(year_year_record)
        session.commit()
        
        # # Team Misc
        # # created_atがあるため更新則はこのまま
        team_misc_soup = soup.find('div', id='div_team_misc').find('tbody')
        team_misc_table = TeamMiscTable(team_misc_soup, self.season, self.team)
        team_misc_team_record, team_misc_lg_rank_record = team_misc_table.get_records()
        if not session.query(TeamMiscTeamRecord).filter(TeamMiscTeamRecord._Season==team_misc_team_record._Season, TeamMiscTeamRecord._abbreviation==team_misc_team_record._abbreviation, TeamMiscTeamRecord._created_at==team_misc_team_record._created_at).all():
            session.add(team_misc_team_record)
        if not session.query(TeamMiscLgRankRecord).filter(TeamMiscLgRankRecord._Season==team_misc_lg_rank_record._Season, TeamMiscLgRankRecord._abbreviation==team_misc_lg_rank_record._abbreviation, TeamMiscLgRankRecord._created_at==team_misc_lg_rank_record._created_at).all():
            session.add(team_misc_lg_rank_record)
        session.commit()

        # ==========================================================================================

        # # PerGame
        # 最初に更新すべき _Season, _abbreviation, _Player を消してから
        per_game_soup = soup.find('div', id='all_per_game-playoffs_per_game').find('tbody')
        per_game_table = PerGameTable(per_game_soup, self.season, self.team)
        for record in per_game_table.get_records():
            if not session.query(PerGameRecord).filter(PerGameRecord._Season==record._Season, PerGameRecord._abbreviation==record._abbreviation, PerGameRecord._Player==record._Player).all():
                print(record.__dict__.items())
                session.add(record)
                session.commit()

        # Totals
        # 最初に更新すべき _Season, _abbreviation, _Player を消してから
        totals_soup = soup.find('div', id='div_totals').find('tbody')
        totals_table = TotalsTable(totals_soup, self.season, self.team)
        for record in totals_table.get_records():
            if not session.query(TotalsRecord).filter(TotalsRecord._Season==record._Season, TotalsRecord._abbreviation==record._abbreviation, TotalsRecord._Player==record._Player).all():
                session.add(record)
                session.commit()

        # Per 36 Minutes
        per_36_minutes_soup = soup.find('div', id='div_per_minute').find('tbody')
        per_36_minutes_table = Per36MinutesTable(per_36_minutes_soup, self.season, self.team)
        for record in per_36_minutes_table.get_records():
            if not session.query(Per36MinutesRecord).filter(Per36MinutesRecord._Season==record._Season, Per36MinutesRecord._abbreviation==record._abbreviation, Per36MinutesRecord._Player==record._Player).all():
                session.add(record)
                session.commit()

        # Per 100 poss
        per_100_poss_soup = soup.find('div', id='div_per_poss').find('tbody')
        per_100_poss_table = Per100PossTable(per_100_poss_soup, self.season, self.team)
        for record in per_100_poss_table.get_records():
            if not session.query(Per100PossRecord).filter(Per100PossRecord._Season==record._Season, Per100PossRecord._abbreviation==record._abbreviation, Per100PossRecord._Player==record._Player).all():
                session.add(record)
                session.commit()


        # Advanced
        advanced_soup = soup.find('div', id='div_advanced').find('tbody')
        advanced_table = AdvancedTable(advanced_soup, self.season, self.team)
        for record in advanced_table.get_records():
            if not session.query(AdvancedRecord).filter(AdvancedRecord._Season==record._Season, AdvancedRecord._abbreviation==record._abbreviation, AdvancedRecord._Player==record._Player).all():
                session.add(record)
                session.commit()                

        # # Play by Play
        play_by_play_soup = soup.find('div', id='div_pbp').find('tbody')
        play_by_play_table = PlayByPlayTable(play_by_play_soup, self.season, self.team)
        for record in play_by_play_table.get_records():
            if not session.query(PlayByPlayRecord).filter(PlayByPlayRecord._Season==record._Season, PlayByPlayRecord._abbreviation==record._abbreviation, PlayByPlayRecord._PositionEstimate__Player==record._PositionEstimate__Player).all():
                session.add(record)
                session.commit()

# 基本情報
class TeamInfoRecord(Base):
    __tablename__ = f'team_stats__info'
    _abbreviation = Column(String(120), primary_key=True)
    _name = Column(String(120))
    _conference = Column(String(4))
    _logo_link = Column(String(120))

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
    
    def __init__(self, soup, season, team):
        self.table_soup = soup
        self.season = season
        self.team = team

    def get_records(self):
        def _clean(k, v):
            if k == '_Player':
                return v.replace('(TW)', '')
            return v
        for  _tr in self.table_soup.find_all('tr'):
            _No = _tr.find('th').text
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k:_clean(k, v) for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_No'] = _No
            record = RosterRecord(**info)
            yield record

class RosterRecord(Base):
    __tablename__ = f'team_stats__roster'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _No = Column(String(16)) # 2つ以上背番号を持つ選手用に
    _Player = Column(String(120), primary_key=True)
    _Pos = Column(String(16))
    _Ht = Column(String(16))
    _Wt = Column(Integer)
    _BirthDate = Column(String(120)) # Date型にしたい
    _CountryCode = Column(String(16)) # usなど
    _Exp = Column(String(4)) # Rは謎
    _College = Column(String(120))

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
            td_list = [_.text for _ in _tr.find_all('td')]
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_Player'] = _Player
            info['_abbreviation'] = self.team.abbreviation
            record = InjuryReportRecord(**info)
            yield record

class InjuryReportRecord(Base):
    __tablename__ = f'team_stats__injury_reports'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120), primary_key=True)
    _Team = Column(String(120))
    _Update = Column(String(120), primary_key=True)
    _Description = Column(String(256))

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Team and Opponent Stats
@dataclass
class TeamAndOpponentStatsTable:
    table_soup: Any
    season: str
    team: Team
    is_opponent: bool
    _keys = ['_G', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']

    def get_records(self):
        def _get_info(td_list, is_team_row=False):
            info = {k: v.replace('%', '').replace('+', '') for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = NOW.strftime('%Y-%m-%d')
            if not is_team_row:
                del info['_G']
            return info

        if not self.is_opponent:
            tr_list = self.table_soup.find_all('tr')
            Team_row, Team_G_row, Lg_Rank_row, Year_Year_row = tr_list
            # Team
            info = _get_info([_.text for _ in Team_row.find_all('td')], True)
            team_record = TeamAndOpponentStatsTeamRecord(**info)
            # Team/G
            info = _get_info([_.text for _ in Team_G_row.find_all('td')])
            team_g_record = TeamAndOpponentStatsTeamGRecord(**info)
            # Lg Rank
            info = _get_info([_.text for _ in Lg_Rank_row.find_all('td')])
            lg_rank_record = TeamAndOpponentStatsLgRankRecord(**info)
            # Year/Year
            info = _get_info([_.text for _ in Year_Year_row.find_all('td')])
            year_year_record = TeamAndOpponentStatsYearYearRecord(**info)
            # 各チーム1行ずつのみなので yield ではなく return を使う
            return team_record, team_g_record, lg_rank_record, year_year_record
        else:
            tr_list = self.table_soup.find_all('tr')
            Opponent_row, Opponent_G_row, Lg_Rank_row, Year_Year_row = tr_list
            # Opponent
            info = _get_info([_.text for _ in Opponent_row.find_all('td')], True)
            opponent_record = TeamAndOpponentStatsOpponentRecord(**info)
            # Opponent/G
            info = _get_info([_.text for _ in Opponent_G_row.find_all('td')])
            opponent_g_record = TeamAndOpponentStatsOpponentGRecord(**info)
            # Lg Rank
            info = _get_info([_.text for _ in Lg_Rank_row.find_all('td')])
            lg_rank_record = TeamAndOpponentStatsLgRankRecord_Opponent(**info)
            # Year/Year
            info = _get_info([_.text for _ in Year_Year_row.find_all('td')])
            year_year_record = TeamAndOpponentStatsYearYearRecord_Opponent(**info)
            # 各チーム1行ずつのみなので yield ではなく return を使う
            return opponent_record, opponent_g_record, lg_rank_record, year_year_record

# 1
class TeamAndOpponentStatsTeamRecord(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Team'
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

# 2
class TeamAndOpponentStatsTeamGRecord(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Team_per_G'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    # _G = Column(Integer)
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

# 3
class TeamAndOpponentStatsLgRankRecord(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Lg_Rank'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    # _G = Column(Integer)
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

# 4
class TeamAndOpponentStatsYearYearRecord(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Year_per_Year'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    # _G = Column(Float)
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



# 1
class TeamAndOpponentStatsOpponentRecord(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Opponent'
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

# 2
class TeamAndOpponentStatsOpponentGRecord(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Opponent_per_G'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    # _G = Column(Integer)
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

# 3
class TeamAndOpponentStatsLgRankRecord_Opponent(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Lg_Rank__Opponent'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    # _G = Column(Integer)
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

# 4
class TeamAndOpponentStatsYearYearRecord_Opponent(Base):
    __tablename__ = f'team_stats__team_and_opponent_stats__Year_per_Year__Opponent'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    # _G = Column(Float)
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
class TeamMiscTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_W', '_L', '_PW', '_PL', '_MOV', '_SOS', '_SRS', '_ORtg', '_DRtg', '_Pace', '_Advanced__FTr', '_Advanced__3PAr', '_Offence_Four_Factor__eFG_percent', '_Offence_Four_Factor__TOV_percent', '_Offence_Four_Factor__ORB_percent', '_Offence_Four_Factor__FT_FGA', '_Defense_Four_Factor__eFG_percent', '_Defense_Four_Factor__TOV_percent', '_Defense_Four_Factor__DRB_percent', '_Defense_Four_Factor__FT_FGA', '_Arena', '_Attendance']

    def get_records(self):
        def _get_info(td_list):
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            info['_created_at'] = NOW.strftime('%Y-%m-%d')
            return info
        
        tr_list = self.table_soup.find_all('tr')
        team_row, lg_rank_row = tr_list
        info = _get_info([_.text for _ in team_row.find_all('td')])
        team_misc_team_record = TeamMiscTeamRecord(**info)
        
        info = _get_info([_.text for _ in lg_rank_row.find_all('td')])
        team_misc_lg_rank_record = TeamMiscLgRankRecord(**info)

        return team_misc_team_record, team_misc_lg_rank_record

class TeamMiscTeamRecord(Base):
    __tablename__ = f'team_stats__team_misc__team'
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
    __tablename__ = f'team_stats__team_misc__lg_rank'
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
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            record = PerGameRecord(**info)
            yield record

# _Rk は関係ない
class PerGameRecord(Base):
    __tablename__ = f'team_stats__per_game'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120), primary_key=True)
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
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            print("=============================================-")
            print(info)
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            record = TotalsRecord(**info)
            yield record

class TotalsRecord(Base):
    __tablename__ = f'team_stats__totals'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120), primary_key=True)
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

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class Per36MinutesTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            record = Per36MinutesRecord(**info)
            yield record

class Per36MinutesRecord(Base):
    __tablename__ = f'team_stats__per36_minutes'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120), primary_key=True)
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

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class Per100PossTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_Player', '_Age', '_G', '_GS', '_MP', '_FG', '_FGA', '_FG_percent', '_3P', '_3PA', '_3P_percent', '_2P', '_2PA', '_2P_percent', '_FT', '_FTA', '_FT_percent', '_ORB', '_DRB', '_TRB', '_AST', '_STL', '_BLK', '_TOV', '_PF', '_PTS', '_tmp', '_ORtg', '_DRtg']

    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text for _ in _tr.find_all('td')]
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            del info['_tmp']
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            record = Per100PossRecord(**info)
            yield record

class Per100PossRecord(Base):
    __tablename__ = f'team_stats__per100_poss'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120), primary_key=True)
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
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            del info['_tmp']
            del info['__tmp']
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation            
            record = AdvancedRecord(**info)
            yield record

class AdvancedRecord(Base):
    __tablename__ = f'team_stats__advanced'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _Player = Column(String(120), primary_key=True)
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

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@dataclass
class PlayByPlayTable:
    table_soup: Any
    season: str
    team: Team
    _keys = ['_PositionEstimate__Player', '_PositionEstimate__Age', 
            '_PositionEstimate__G', '_PositionEstimate__MP', 
            '_PositionEstimate__PG_percent', '_PositionEstimate__SG_percent', '_PositionEstimate__SF_percent', '_PositionEstimate__PF_percent', '_PositionEstimate__C_percent',
            '_PlusMinus_Per_100_Poss__OnCourt', '_PlusMinus_Per_100_Poss__OnOff', 
            '_Turnovers__BadPass', '_Turnovers__LostBall', 
            '_Fouls_Committed__Shoot', '_Fouls_Committed__Off', 
            '_Fouls_Drawn__Shoot', '_Fouls_Drawn__Off', 
            '_Misc__PGA', '_Misc__And1', '_Misc__Blkd']
    def get_records(self):
        for _tr in self.table_soup.find_all('tr'):
            td_list = [_.text.replace('%', '') for _ in _tr.find_all('td')]
            if not td_list:
                continue
            info = {k: v for k, v in zip(self._keys, td_list)}
            info['_Season'] = self.season
            info['_abbreviation'] = self.team.abbreviation
            del_target_set = set()
            for k, v in info.items():
                if v!=0. and not v:
                    del_target_set.add(k)
            for k in del_target_set:
                del info[k]
            record = PlayByPlayRecord(**info)
            yield record
    
class PlayByPlayRecord(Base):
    __tablename__ = f'team_stats__play_by_play'
    _Season = Column(String(120), primary_key=True)
    _abbreviation = Column(String(120), primary_key=True)
    _PositionEstimate__Player = Column(String(120), primary_key=True)
    _PositionEstimate__Age = Column(Integer)
    _PositionEstimate__G = Column(Integer)
    _PositionEstimate__MP = Column(Integer)
    _PositionEstimate__PG_percent = Column(Integer)
    _PositionEstimate__SG_percent = Column(Integer)
    _PositionEstimate__SF_percent = Column(Integer)
    _PositionEstimate__PF_percent =Column(Integer)
    _PositionEstimate__C_percent = Column(Integer)
    _PlusMinus_Per_100_Poss__OnCourt = Column(Float) 
    _PlusMinus_Per_100_Poss__OnOff = Column(Float)
    _Turnovers__BadPass = Column(Integer)
    _Turnovers__LostBall = Column(Integer)
    _Fouls_Committed__Shoot = Column(Integer)
    _Fouls_Committed__Off = Column(Integer)
    _Fouls_Drawn__Shoot = Column(Integer)
    _Fouls_Drawn__Off = Column(Integer)
    _Misc__PGA = Column(Integer)
    _Misc__And1 = Column(Integer) 
    _Misc__Blkd = Column(Integer)
    
if __name__ == '__main__':
    _Season = 2021
    # 削除プロトコル
    session.query(PerGameRecord).filter(PerGameRecord._Season == CURRENT_SEASON).delete()
    session.query(TotalsRecord).filter(TotalsRecord._Season == CURRENT_SEASON).delete()
    session.query(Per36MinutesRecord).filter(Per36MinutesRecord._Season == CURRENT_SEASON).delete()
    session.query(Per100PossRecord).filter(Per100PossRecord._Season == CURRENT_SEASON).delete()
    session.query(AdvancedRecord).filter(AdvancedRecord._Season == CURRENT_SEASON).delete()
    session.query(PlayByPlayRecord).filter(PlayByPlayRecord._Season == CURRENT_SEASON).delete()
    session.commit()


    for team in TEAMS_SET:
    
        team_page = TeamPage(team, _Season)
        # team_page.create_table()
        team_page.update_latest_records()



    
    # for index, _player, player_overview_url in session.query(AllPlayersRecord.id, AllPlayersRecord._player, AllPlayersRecord._url).all():
    #     # tm が異なってもGameLogには1シーズン情報で出てくるので.first()でOK
    #     for res in session.query(distinct(PerGameRecord._Season)).filter(PerGameRecord.id == index).all():
    #         _Season = res[0]
