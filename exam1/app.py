from contextlib import contextmanager
import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Records(Base):
    __tablename__ = 'records'

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    iid=sa.Column(sa.SmallInteger)
    order_id=sa.Column(sa.Integer)
    cost_us=sa.Column(sa.REAL)
    cost_rus=sa.Column(sa.REAL)
    date=sa.Column(sa.Date)

engine = create_engine(
    'postgresql+psycopg2://utest:utest@localhost:5432/kanalservice',
    echo=False,
    isolation_level='READ COMMITTED'
)
engine.connect()

DBSession = sessionmaker(
    binds={
        Base: engine,
    },
    expire_on_commit=False,
)

@contextmanager
def session_scope():
    """Provides a transactional scope around a series of operations."""
    session = DBSession()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

if __name__ == '__main__':
    with session_scope() as s:
        # query = s.query(Records).filter(
        #     Records.iid < 5,
        # ).order_by(Records.id.desc()).limit(10).all()
        # for res in query:
        #     print(res.iid,)

        ins = Records(iid=19, order_id=1234, cost_us=1234.3, cost_rus=2345.3, date=datetime.date(year=2022, month=8, day=7))
        s.add(ins)
        # s.commit()


        query = s.query(Records).all()
        for q in query:
            print(q.iid, q.date)
