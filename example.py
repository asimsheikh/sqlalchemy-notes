from sqlalchemy import create_engine, text
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.orm import Session

from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.orm import selectinload
from sqlalchemy import Column, Integer, String, ForeignKey

Base = declarative_base()

class User(Base):
    __tablename__ = 'user_account'
    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)
    addresses = relationship('Address', back_populates='user')
    
    def __repr__(self):
        return f'User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r}'
    
class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('user_account.id'))
    user = relationship('User', back_populates='addresses')
    
    def __repr__(self):
        return f'Address(id={self.id!r}, email_address={self.email_address!r}'

class Repo:
    def __init__(self):
        self.engine = create_engine('sqlite+pysqlite:///:memory:', echo=False, future=True)
        Base.metadata.create_all(self.engine)
        
    def add_user(self, user: User):
        with Session(self.engine) as session:
            session.add(user)
            session.commit()
            
    def get_user(self, name: str, fullname: str) -> CursorResult:
        with Session(self.engine) as session:
            stmt = select(User).where(User.name == name)
            result = session.execute(stmt)
            return result.all()
        
    def add_user_address(self, user: User, address: Address):
        user = user
        user.addresses.append(address)
        
        with Session(self.engine) as session:
            session.add(user)
            session.commit()
            
    def get_users_addressess(self):
        stmt = select(User).options(selectinload(User.addresses)).order_by(User.id)
        with Session(self.engine) as session:
            result = session.execute(stmt)
            return result.all()

    def exec(self, stmt: str) -> CursorResult:
        with Session(self.engine) as session:
            result = session.execute(stmt)
            return result.all()
