import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Text, DateTime, Sequence, ForeignKey, create_engine, MetaData, DECIMAL, DATETIME, exc, event, Index
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from dataclasses import dataclass
from all_players import AllPlayersRecord
from utils import ALPHABET, HOST, DB_NAME
from utils import get_mysql_pass, get_soup_by_url
Base = declarative_base()
USER, PASSWD = get_mysql_pass()
DB_NAME = 'NBA3'
DATABASE = f'mysql://{USER}:{PASSWD}@{HOST}/{DB_NAME}?charset=utf8'
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=True)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=ENGINE))

@dataclass
class PlayerInfoPage():
    id: int
    player: str
    url: str

    def get_info(self):
        soup = get_soup_by_url(self.url, False)
        image_block_soup = soup.find('div', id='info', class_='players')
        if image_block_soup and image_block_soup.find('img'):
            img_url = image_block_soup.find('img').get('src')            
            info = {'id': self.id, '_Player': self.player, '_image_url': img_url}
            record = AllPlayerImageRecord(**info)
            session.add(record)
            session.commit()

class AllPlayerImageRecord(Base):
    __tablename__ = 'all_player__images'
    id = Column(Integer, primary_key=True)
    _Player = Column(String(120))
    _image_url = Column(String(256))

if __name__ == '__main__':
    Base.metadata.create_all(bind=ENGINE)
    for index, _player, _url in session.query(AllPlayersRecord.id, AllPlayersRecord._player, AllPlayersRecord._url).all():
        
        if not session.query(AllPlayerImageRecord._image_url).filter(AllPlayerImageRecord.id == index).all():
            print(index, _player, _url)    
            page = PlayerInfoPage(index, _player, _url)
            page.get_info()


# updateは一回消してから
# python3 all_player_picture.py