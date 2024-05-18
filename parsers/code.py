import logging

import MySQLdb
from sqlalchemy.exc import OperationalError

from sqlalchemy.orm import sessionmaker
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from tqdm import tqdm

from db.everything.queries import get_max_user_url, get_object_with_empty_field, \
    update_title, add_full_user
from parsers.data_classes import UserIsInactiveException

from parsers.help_functions import start_driver
from parsers.parsers_funcs import parse_user_info, parse_title


def get_users_info(driver: ChromiumDriver,
                   session_factory: sessionmaker,
                   user_ids: list = None,
                   amount: int = 1) -> None:
    if not user_ids:
        last_used_url = get_max_user_url(session_factory)
        user_ids = range(last_used_url + 1, last_used_url + amount + 1)

    for i, user_id in tqdm(enumerate(user_ids)):
        try:
            full_url = 'https://mangalib.me/user/' + str(user_id)
            user_data = parse_user_info(driver, full_url)
            if user_data:
                add_full_user(session_factory, user_data)
        except UserIsInactiveException as e:
            pass
        except Exception as e:
            logging.error('Неизвестная ошибка. https://mangalib.me/user/' + str(user_id), e)


def get_books_info(driver: ChromiumDriver,
                   session_factory: sessionmaker,
                   use_db: bool = False):
    if use_db:
        title_urls = get_object_with_empty_field(session_factory)
    else:
        return

    for url in tqdm(title_urls):
        title_info = parse_title(driver, 'https://mangalib.me/' + url)
        if title_info:
            title_info['url'] = url
            update_title(session_factory, title_info)
        else:
            print(url, 'не удалось использовать')


