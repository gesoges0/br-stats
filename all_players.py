import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Text, DateTime, Sequence, ForeignKey, create_engine, MetaData, DECIMAL, DATETIME, exc, event, Index
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.ext.declarative import declarative_base

import requests
from bs4 import BeautifulSoup
from pathlib import Path
from utils import ALPHABET, HOST, DB_NAME
from utils import get_mysql_pass, get_soup_by_url
from typing import Any, List

Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DB_NAME = 'NBA3'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

class AllPlayersPage():
    def __init__(self):
        pass

    def create_table(args):
        """tableを作成する"""
        # Base.metadata.create_all(bind=ENGINE)
        Base.metadata.create_all(bind=ENGINE, tables=[AllPlayersRecord.__table__])

    def update_all_players_table(self):
        """選手テーブルを更新する"""
        for alpha in ALPHABET:
            url = f'https://www.basketball-reference.com/players/{alpha}/'
            all_players_table = AllPlayersTable(url)
            for player in all_players_table.get_all_players_in_a_page():
                if not session.query(AllPlayersRecord).filter(AllPlayersRecord._player == player._player, AllPlayersRecord._from == player._from, AllPlayersRecord._to == player._to).all():
                    session.add(player)
                    session.commit()    

class AllPlayersTable():
    """Web側のtable"""
    url: str
    table: Any
    all_players: List[Any]

    def __init__(self, url):
        self.url = url
        # res = requests.get(self.url)
        # soup = BeautifulSoup(res.text, 'html.parser')
        soup = get_soup_by_url(self.url, False)
        self.table = soup.find('tbody')

    def get_all_players_in_a_page(self):
        """Web側の一つのページにある選手情報を返すgenerator"""
        for _tr in self.table.find_all('tr'):
            if _tr.get('class'):
                continue
            _th = _tr.find('th').find('a')
            _player_url, _player_name = 'https://www.basketball-reference.com' + _th.get('href'), _th.text
            _From, _To, _Pos, _Ht, _Wt, _Birth_Date, _Colleges = [_.text for _ in _tr.find_all('td')]
            _url_for_player = _player_url.replace('.', '').replace('html', '').split('/')[-1]
            player_record = AllPlayersRecord(_player_name, _From, _To, _Pos, _Ht, _Wt, _Birth_Date, _Colleges, _player_url, _url_for_player)
            yield player_record

class AllPlayersRecord(Base):
    """
    all_players tableの1recordを表す
    https://www.basketball-reference.com/players/a/
    """
    __tablename__ = 'all_players'
    id = Column(Integer, primary_key=True, autoincrement=True)
    _player = Column(String(120))# , unique=True) <---- Mike James の件で消した
    _from = Column(Integer)
    _to = Column(Integer)
    _pos = Column(String(10))
    _ht = Column(String(10))
    _wt = Column(Integer)
    _birth_date = Column(String(36))
    _colleges = Column(String(120))
    _url = Column(String(120))
    _url_for_player = Column(String(120))

    def __init__(self, _player, _from, _to, _pos, _ht, _wt, _birth_date, _colleges, _url, _url_for_player):
        self._player = _player
        self._from = _from 
        self._to = _to
        self._pos = _pos
        self._ht = _ht
        self._wt = _wt
        self._birth_date = _birth_date
        self._colleges = _colleges
        self._url = _url
        self._url_for_player = _url_for_player


if __name__ == '__main__':
    all_players_page = AllPlayersPage()
    all_players_page.update_all_players_table()