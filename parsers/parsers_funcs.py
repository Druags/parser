import selenium.webdriver.chromium.webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from creds import mail, password
from parsers.data_classes import UserInfo, UserIsInactiveException, PageNotFound
from parsers.help_functions import get_user_sex, get_user_favorite_tags, user_is_active, get_manga_info_list, \
    get_manga_statistics, convert_book_info, scroll_down
from selenium.common.exceptions import NoSuchElementException


def authorize(driver):
    driver.get('https://lib.social/login?from=https%3A%2F%2Fmangalib.me%2F')

    driver.find_element(By.XPATH,
                        '//*[@id="site_type"]/body/div[1]/div/div/div[2]/div[1]/form/div[2]/div/input').send_keys(mail)
    driver.find_element(By.XPATH,
                        '//*[@id="site_type"]/body/div[1]/div/div/div[2]/div[1]/form/div[3]/div/input').send_keys(
        password)
    driver.find_element(By.XPATH,
                        '//*[@id="site_type"]/body/div[1]/div/div/div[2]/div[1]/form/div[5]/button').click()


def parse_user_info(driver, user_url: str):
    driver.get(user_url + '?folder=5')
    if '404' in driver.title:
        raise PageNotFound('Страница пользователя недоступна или заблокирована')

    WebDriverWait(driver, 20).until_not(
        EC.presence_of_element_located((By.CLASS_NAME, 'loader-wrapper'))
    )

    soup = BeautifulSoup(driver.page_source, features="html.parser")
    if not user_is_active(soup):
        raise UserIsInactiveException('Пользователь не соответствует поставленным требованиям')

    favorite_tags = get_user_favorite_tags(soup)
    sex = get_user_sex(soup)
    favorites_links = parse_user_mangas(driver, user_url + '?folder=5')
    abandoned_links = parse_user_mangas(driver, user_url + '?folder=3')

    return UserInfo(sex, favorite_tags, favorites_links, abandoned_links)


def parse_book(driver: selenium.webdriver.chromium.webdriver.ChromiumDriver,
               link: str):
    driver.get(link)

    if '404' in driver.title:
        raise PageNotFound('Страница не найдена')

    if not driver.find_element(By.CLASS_NAME, 'modal__header'):
        print('Не найден modal')
        return

    try:
        more_button = driver.find_element(By.CLASS_NAME, 'media-tag-item_more')
        more_button.click()
    except NoSuchElementException as e:
        print('Кнопка расширения списка тегов не найдена')

    soup = BeautifulSoup(driver.page_source, features="html.parser")

    tags = soup.find('div', {'class': 'media-tags'})
    if tags:
        tags = [tag.text for tag in tags.find_all('a', {'class': 'media-tag-item'})]

    info_list = get_manga_info_list(soup)
    in_lists, ratings = get_manga_statistics(soup)
    book_info = convert_book_info(tags, info_list, ratings)
    return book_info


# TODO парсит дубликаты
def parse_user_mangas(driver, user_url: str) -> set:
    links = set()
    try:
        driver.get(user_url)
        scroll_down(driver)
        soup = BeautifulSoup(driver.page_source, features="html.parser")
        bookmarks = (soup.find('div', {'class': "bookmark__list"})
                     .find_all('div', {'class': 'bookmark-item'}))
        for bookmark in bookmarks:
            raw_link = bookmark.find('div', {'class': 'bookmark-item__info-header'}).find('a').get('href')
            clean_link = 'https://mangalib.me' + raw_link.split('?')[0]
            if clean_link not in links:
                links.add(clean_link)
    except Exception as e:
        if '?folder=5' in user_url:
            print('Неизвестная ошибка при поиске любимых тайтлов')
        elif '?folder=3' in user_url:
            print('Неизвестная ошибка при поиске брошеных тайтлов')
        print(e)

    return links
