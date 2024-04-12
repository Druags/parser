import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from queries import create_tables, fill_tables, add_connections
from db.config import settings


def main() -> None:
    mysql_engine = create_engine(
        url=settings.DATABASE_URL_mysql
    )

    session_factory = sessionmaker(mysql_engine)
    # create_tables(mysql_engine)
    # fill_tables(session_factory)
    # add_connections(session_factory)


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    work_time = end_time - start_time
    print(f'Время работы программы: {int(work_time // 3600)}:{int(work_time // 60 % 60)}:{round(work_time % 60, 2)}')
