import time

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC

from db.everything.data_work import fix_year


def get_user_sex(soup: BeautifulSoup) -> int:
    try:
        page_modals = soup.find('div', {'class': 'page-modals'})
        profile_info = page_modals.find('table', {'class': 'profile-info'})
        info_row = profile_info.find_all('tr', {'class': 'profile-info__row'})[1]
        sex = info_row.find('td', {'class': 'profile-info__value'}).text
        if sex == 'Женский':
            sex = 0
        elif sex == 'Мужской':
            sex = 1
    except IndexError:
        sex = 2
    except AttributeError:
        sex = 2
    except Exception as e:
        sex = 2
        print('Неизвестная ошибка при поиске пола', e)
    return sex


def get_title_info_list(soup):
    try:
        keys = (soup.find('div', {'class': 'media-info-list'}).
                find_all('div', {'class': 'media-info-list__title'}))

        values = (soup.find('div', {'class': 'media-info-list'}).
                  find_all('div', {'class': 'media-info-list__value'}))
    except Exception as e:
        print('Неизвестная ошибка', e)
        return

    try:
        info_list = {key.text: value.text.strip() for key, value in zip(keys, values)}
    except Exception as e:
        print('Неизвестная ошибка', e)
        info_list = {}

    return info_list


def convert_title_info(tags, info_list, ratings) -> dict:

    book_info = {'type': info_list['Тип'],
                 'tags': set(tag.lower() for tag in tags),
                 'ratings': ratings,
                 'release_year': fix_year(info_list.get('Год релиза')),
                 'publication_status': info_list.get('Статус тайтла'),
                 'translation_status': info_list.get('Статус перевода'),
                 'authors': set(author.lower() for author in info_list.get('Автор', '').split('\n')),
                 'artists': set(artist.lower() for artist in info_list.get('Художник', '').split('\n')),
                 'publishers': set(publisher.lower() for publisher in info_list.get('Издательство', '').split('\n')),
                 'chapters_uploaded': info_list.get('Загружено глав'),
                 'age_rating': info_list.get('Возрастной рейтинг'),
                 'release_formats': set(r_format.lower() for r_format in info_list.get('Формат выпуска', '').split('\n'))}

    return book_info


def get_manga_statistics(soup) -> (dict, dict):
    lists = ['Читаю', 'В планах', 'Брошено', 'Прочитано', 'Любимое', 'Другое']
    stars = range(10, 0, -1)
    try:
        statistics_div = (soup.find('div', {'class': 'media-section_stats'}).
                          find_all('div', {'class': 'media-section__col'}))
    except AttributeError as e:
        print(e)
        return

    try:
        in_lists = {key: int(value.text) for key, value in
                    zip(lists, statistics_div[0].find_all('div', {'class': 'media-stats-item__count'}))}
    except IndexError:
        in_lists = {}

    try:
        ratings = {key: int(value.text.strip()) for key, value in
                   zip(stars, statistics_div[1].find_all('div', {'class': 'media-stats-item__count'}))}
    except IndexError:
        ratings = {}

    return in_lists, ratings


def get_user_favorite_tags(soup: BeautifulSoup) -> dict:
    genres = soup.find('div', {'class': 'page-modals'}). \
        find('div', {'class': 'user-genres'}). \
        find_all('div', {'class': 'user-genres__genre'})
    genres_amount = {}
    for genre in genres:
        genre_name = genre.find('div', {'class': 'user-genres__name'}).text.strip()
        genre_amount = genre.find('div', {'class': 'user-genres__amount'}).text.strip()
        genres_amount[genre_name] = genre_amount
    return genres_amount


def user_is_active(soup) -> bool:
    try:
        bookmark_menu = soup.find('div', {'class': 'bookmark-menu'})
        bookmarks = bookmark_menu.find_all('div', {'class': 'menu__item'})
        books_n = int(bookmarks[0].find('span', {'class': 'bookmark-menu__label'}).text)
        books_favorite_n = int(bookmarks[5].find('span', {'class': 'bookmark-menu__label'}).text)

        if not (books_favorite_n == 0 or books_n < 50):
            return True
    except Exception as e:
        print('Неизвестная ошибка', e)


def start_driver(undetected=True):
    if undetected:
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument("--disable-cache")
        proxy_options = {
            'proxy': {
                'http': 'http://user:pass@ip:port',
                'https': 'https://user:pass@ip:port',
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        driver = uc.Chrome(
            options=chrome_options,
            seleniumwire_options=proxy_options,
            driver_executable_path=ChromeDriverManager().install(),
            use_subprocess=False,
            no_sandbox=False
        )
    else:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument("--disable-cache")
        driver = webdriver.Chrome(chrome_options)
    return driver


def scroll_down(driver):
    max_scrolls = 10000
    scroll_count = 0
    prev_height = -1
    WebDriverWait(driver, 20).until_not(
        EC.presence_of_element_located((By.CLASS_NAME, 'loader-wrapper'))
    )
    while scroll_count < max_scrolls:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == prev_height:
            break
        prev_height = new_height
        scroll_count += 1
