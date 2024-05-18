import selenium.webdriver.chromium.webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from creds import mail, password
from parsers.data_classes import UserInfo, UserIsInactiveException, PageNotFound
from parsers.help_functions import get_user_sex, get_user_favorite_tags, user_is_active, get_title_info_list, \
    get_manga_statistics, convert_title_info, scroll_down
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException


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
        return

    WebDriverWait(driver, 20).until_not(
        EC.presence_of_element_located((By.CLASS_NAME, 'loader-wrapper'))
    )

    soup = BeautifulSoup(driver.page_source, features="html.parser")
    if not user_is_active(soup):
        return

    favorite_tags = get_user_favorite_tags(soup)
    sex = get_user_sex(soup)
    favorites_links = parse_user_mangas(driver, user_url + '?folder=5')
    abandoned_links = parse_user_mangas(driver, user_url + '?folder=3')
    url = int(user_url.split('/')[-1])

    return UserInfo(url, sex, favorite_tags, favorites_links, abandoned_links)


def parse_title(driver: selenium.webdriver.chromium.webdriver.ChromiumDriver,
                url: str):
    driver.get(url)

    if '404' in driver.title:
        print(url, 'Страница не найдена')
        return

    if not driver.find_element(By.CLASS_NAME, 'modal__header'):
        print(url, 'modal не найден')
        return

    try:
        more_button = driver.find_element(By.CLASS_NAME, 'media-tag-item_more')
        more_button.click()
    except NoSuchElementException:
        pass
    except ElementNotInteractableException:
        pass
    except Exception as e:
        print(url, 'Неизвестная ошибка', e)

    soup = BeautifulSoup(driver.page_source, features="html.parser")

    tags = soup.find('div', {'class': 'media-tags'})
    if tags:
        tags = [tag.text for tag in tags.find_all('a', {'class': 'media-tag-item'})]

    info_list = get_title_info_list(soup)
    if not info_list:
        print(url, 'нет информации')
        return

    in_lists, ratings = get_manga_statistics(soup)
    title_info = convert_title_info(tags, info_list, ratings)
    return title_info


# TODO парсит дубликаты
def parse_user_mangas(driver, user_url: str) -> set:
    title_urls = set()
    try:
        driver.get(user_url)
        scroll_down(driver)
        soup = BeautifulSoup(driver.page_source, features="html.parser")
        bookmarks = (soup.find('div', {'class': "bookmark__list"})
                     .find_all('div', {'class': 'bookmark-item'}))
        for bookmark in bookmarks:
            raw_link = bookmark.find('div', {'class': 'bookmark-item__info-header'}).find('a').get('href')
            title_url = raw_link.split('?')[0][1:]
            if title_url not in title_urls:
                title_urls.add(title_url)
    except Exception as e:
        if '?folder=5' in user_url:
            print('Неизвестная ошибка при поиске любимых тайтлов')
        elif '?folder=3' in user_url:
            print('Неизвестная ошибка при поиске брошеных тайтлов')
        print(e)

    return title_urls
