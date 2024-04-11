from sqlalchemy import create_engine, Table, URL
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from db.config import settings

mysql_engine = create_engine(
    url=settings.DATABASE_URL_mysql,
    # echo=True,
    # pool_size=5,
    # max_overflow=10
)

session_factory = sessionmaker(mysql_engine)


class Base(DeclarativeBase):
    def __repr__(self):
        """Relationships не используются в repr(), т.к. могут вести к неожиданным подгрузкам"""
        self.repr_cols_num = 3
        self.repr_cols = tuple()
        cols = []
        for idx, col in enumerate(self.__table__.columns.keys()):
            if col in self.repr_cols or idx < self.repr_cols_num:
                cols.append(f"{col}={getattr(self, col)}")

        return f"<{self.__class__.__name__} {', '.join(cols)}>"





