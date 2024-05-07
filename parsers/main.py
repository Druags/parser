import os
import signal
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import settings
from parsers.code import start_driver, get_users_info


def main():
    if not (os.path.exists('data') and os.path.isdir('data')):
        os.mkdir('data')
    driver = start_driver()
    mysql_engine = create_engine(
        url=settings.DATABASE_URL_mysql
    )
    session_factory = sessionmaker(mysql_engine)
    get_users_info(driver, session_factory, amount=100)

    pid = driver.service.process.pid
    driver.close()

    try:
        os.kill(int(pid), signal.SIGTERM)
        print("Killed chrome using process")
    except ProcessLookupError as e:
        print('Не удалось убить процесс', e)


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    work_time = end_time - start_time
    print(f'Время работы программы: {int(work_time // 3600)}:{int(work_time // 60 % 60)}:{round(work_time % 60, 2)}')