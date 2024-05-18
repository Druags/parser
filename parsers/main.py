import os
import signal
import time
import logging

from sqlalchemy import create_engine, text

from sqlalchemy.orm import sessionmaker

from config import settings, ROOT_PATH
from parsers.code import start_driver, get_users_info, get_books_info


def main():
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

    if not (os.path.exists(ROOT_PATH+'data') and os.path.isdir(ROOT_PATH+'data')):
        os.mkdir(ROOT_PATH+'/data')
    mysql_engine = create_engine(
        url=settings.DATABASE_URL_mysql
    )

    try:
        session_factory = sessionmaker(mysql_engine)
        with session_factory() as session:
            session.execute(text('SELECT 1'))
    except Exception as e:
        raise e

    driver = start_driver()
    get_users_info(driver, session_factory, amount=100000)
    # get_books_info(driver, session_factory, use_db=True)

    pid = driver.service.process.pid

    try:
        os.kill(int(pid), signal.SIGTERM)
    except ProcessLookupError as e:
        print('Не удалось убить процесс', e)
    except PermissionError as e:
        print('PermissionError: [WinError 5] Отказано в доступе')
    driver.quit()


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    work_time = end_time - start_time
    print(f'Время работы программы: {int(work_time // 3600)}:{int(work_time // 60 % 60)}:{round(work_time % 60, 2)}')