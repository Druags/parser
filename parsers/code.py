import pandas as pd

from sqlalchemy.orm import sessionmaker
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from db.everything.queries import get_max_user_url, add_full_users, get_object_with_empty_field, \
    update_titles
from parsers.data_classes import UserIsInactiveException

from parsers.help_functions import start_driver
from parsers.parsers_funcs import parse_user_info, parse_book


def get_users_info(driver: ChromiumDriver,
                   session_factory: sessionmaker,
                   user_ids: list = None,
                   amount: int = 1) -> None:
    if not user_ids:
        last_used_url = get_max_user_url(session_factory)
        user_ids = range(last_used_url + 1, last_used_url + amount + 1)

    user_info = pd.DataFrame(columns=['url', 'sex', 'favorite_tags', 'favorite_titles', 'abandoned_titles'])
    for i, user_id in enumerate(user_ids):
        if i % 70 == 0:
            driver.quit()
            driver = start_driver()
        try:
            full_url = 'https://mangalib.me/user/' + str(user_id)
            user_data = parse_user_info(driver, full_url)
            if user_data:
                user_info.loc[i] = [user_id, *user_data]
        except UserIsInactiveException as e:
            pass
        except Exception as e:
            print('Неизвестная ошибка. https://mangalib.me/user/' + str(user_id), e)

    add_full_users(session_factory, user_info)
    driver.close()


def get_books_info(driver: ChromiumDriver,
                   session_factory: sessionmaker,
                   use_db: bool = False):
    if use_db:
        title_urls = get_object_with_empty_field(session_factory)
    else:
        title_urls = ['ushiro-no-shoumen-kamui-san']
    titles = pd.DataFrame(columns=['url', 'type', 'tags', 'ratings', 'release_year', 'publication_status',
                                   'translation_status', 'authors', 'artists', 'publishers',
                                   'chapters_uploaded', 'age_rating', 'release_formats'])
    for url in title_urls:
        title_info = parse_book(driver, 'https://mangalib.me/' + url)
        titles.loc[len(titles.index)] = [url, *title_info.values()]

    update_titles(session_factory, titles)
