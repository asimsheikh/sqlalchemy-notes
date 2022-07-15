from sqlalchemy import create_engine
from sqlalchemy import text

engine = create_engine('sqlite+pysqlite:///:memory:', echo=True, future=True)

## basic connection to the database

with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

## commit as you go

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
            text("INSERT INTO some_table (x,y) VALUES (:x, :y)"),
            [dict(x=1, y=1), dict(x=2, y=2)]
    )
    conn.commit()

## working with the result object

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM some_table"))
    print('-'*20)
    for row in result:
        print(f"x: {row.x} y: {row.y}")
    print('-'*20)


## the result object is a namedtuple so we can use

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM some_table"))
    for x,y in result:
        print(f"x: {x} y: {y}")

## or even dictionary mappings 

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM some_table"))
    for dict_row in result.mappings():
        x = dict_row['x']
        y = dict_row['y']


## the ORM approach is to use a session

from sqlalchemy.orm import Session

stmt = text('SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y').bindparams(y=0)
with Session(engine) as session:
    result = session.execute(stmt)
    for row in result:
        print(f"x: {row.x} y: {row.y}")

## Metadata represents the collection of tables in our app

from sqlalchemy import MetaData

metadata_obj = MetaData() 

from sqlalchemy import Table, Column, Integer, String

user_table = Table(
        "user_account", 
        metadata_obj, 
        Column('id', Integer, primary_key=True),
        Column('name', String(30)),
        Column('fullname', String)
)


print(user_table.c.name)
print(user_table.c.keys())
print(user_table.primary_key)


## declaring a foreign key on the table

from sqlalchemy import ForeignKey 

address_table = Table(
        "address", 
        metadata_obj, 
        Column('id', Integer, primary_key=True),
        Column('user_id', ForeignKey('user_account.id'), nullable=False),
        Column('email_address', String, nullable=False)
)

## we send this heirarchy to the databse to emit DDL statements

metadata_obj.create_all(engine)


## defining table metadata using the orm

from sqlalchemy.orm import registry

mapper_registry = registry()
mapper_registry.metadata  # return MetaData()
Base = mapper_registry.generate_base()
# insteady of declaring table objects, we will map them with a base class 

## combine the above steps with 

from sqlalchemy.orm import declarative_base
Base = declarative_base()

## declaring the mapped classes

from sqlalchemy.orm import relationship

class User(Base):
    __tablename__ = 'user_account'
    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)
    addresses = relationship("Address", back_populates="user")
    def __repr__(self): return f'User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r}' 

class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('user_account.id'))
    def __repr__(self): return f'Address(id={self.id!r}, email_address={self.email_address!r}'

print(User.__table__)

## emitting CREATE TABLE statements

# mapper_registry.metadata.create_all(engine)

Base.metadata.create_all(engine)

## inserting data into the database

from sqlalchemy import insert
stmt = insert(user_table).values(name='spongebob', fullname='Spongebob Squarepants')

print(stmt)
print(stmt.compile())

## executing the insert statement

with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()

print(result.inserted_primary_key)

## most common way to issue multiple inserts

with engine.connect() as conn:
    result = conn.execute(
        insert(user_table),
        [dict(name='sandy', fullname='Sandy Cheeks'),
         dict(name='patrick', fullname='Patrick Star')
         ]
    )
    conn.commit()

## basics of selection

from sqlalchemy import select 
stmt = select(user_table).where(user_table.c.name == 'spongebob')
print(stmt)

with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)

## select statement with ORM

stmt = select(User).where(User.name == 'spongebob')
with Session(engine) as session:
    for row in session.execute(stmt):
        print(row)

