import csv
import time

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from creds import mail, password
from parsers.help_functions import get_user_sex, get_user_favorite_tags, user_is_active


def authorize(driver):
    driver.get('https://lib.social/login?from=https%3A%2F%2Fmangalib.me%2F')

    driver.find_element(By.XPATH,
                        '//*[@id="site_type"]/body/div[1]/div/div/div[2]/div[1]/form/div[2]/div/input').send_keys(mail)
    driver.find_element(By.XPATH,
                        '//*[@id="site_type"]/body/div[1]/div/div/div[2]/div[1]/form/div[3]/div/input').send_keys(
        password)
    driver.find_element(By.XPATH,
                        '//*[@id="site_type"]/body/div[1]/div/div/div[2]/div[1]/form/div[5]/button').click()
    time.sleep(3)


def parse_user_info(driver, user_link: str):
    driver.get(user_link + '?folder=5')
    if '404' in driver.title:
        print('404')
        return

    WebDriverWait(driver, 20).until_not(
        EC.presence_of_element_located((By.CLASS_NAME, 'loader-wrapper'))
    )

    soup = BeautifulSoup(driver.page_source, features="html.parser")
    if not user_is_active(soup):
        print('User is inactive')
        return

    favorite_tags = get_user_favorite_tags(soup)
    sex = get_user_sex(soup)
    favorites_links = parse_user_mangas(driver, user_link + '?folder=5')
    abandoned_links = parse_user_mangas(driver, user_link + '?folder=3')

    return sex, favorite_tags, favorites_links, abandoned_links


# todo разбить пользователей на категории по числу прочитанной манги
def check_user_account(driver, user_link):
    driver.get(user_link)
    if '404' not in driver.title:
        WebDriverWait(driver, 20).until_not(
            EC.presence_of_element_located((By.CLASS_NAME, 'loader-wrapper'))
        )
        soup = BeautifulSoup(driver.page_source)
        try:
            bookmarks = soup.find('div', {'class': 'bookmark-menu'}).find_all('div', {'class': 'menu__item'})
            books_n = int(bookmarks[0].find('span', {'class': 'bookmark-menu__label'}).text)
            books_favorite_n = int(bookmarks[5].find('span', {'class': 'bookmark-menu__label'}).text)
            if not (books_favorite_n == 0 or books_n < 50):
                return True
        except Exception as error:
            print(error)
            print(user_link)


def parse_books(driver, books, write_file_name):
    with (open(f'../data/{write_file_name}.csv', 'a', encoding='UTF-8', newline='') as file,
          open('../data/failures.csv', 'a', encoding='UTF-8', newline='') as file_failures):
        csv_writer = csv.writer(file)
        csv_writer_failures = csv.writer(file_failures)
        for book in books:
            try:
                book.set_attrs(*parse_book(driver, book.link))
                csv_writer.writerow(book.get_info().values())
                print('success', book.link)
            except Exception:
                csv_writer_failures.writerow(book.link)
                print('fail', book.link)


lists = ['Читаю', 'В планах', 'Брошено', 'Прочитано', 'Любимое', 'Другое']
stars = range(10, 0, -1)


def parse_book(driver, link):
    driver.get(link)

    if '404' in driver.title or not driver.find_element(By.CLASS_NAME, 'modal__header'):
        driver.get_screenshot_as_file(f'../failures/{link}.png')
        return

    more_button = driver.find_element(By.CLASS_NAME, 'media-tag-item_more')
    if more_button:
        more_button.click()

    soup = BeautifulSoup(driver.page_source)
    name = soup.find('div', {'class': 'media-name__main'}).text.strip()

    alt_name = soup.find('div', {'class': 'media-name__alt'})
    if alt_name:
        alt_name.text.strip()

    tags = soup.find('div', {'class': 'media-tags'})
    if tags:
        tags = [tag.text for tag in tags.find_all('a', {'class': 'media-tag-item'})]

    keys = (soup.find('div', {'class': 'media-info-list'}).
            find_all('div', {'class': 'media-info-list__title'}))

    values = (soup.find('div', {'class': 'media-info-list'}).
              find_all('div', {'class': 'media-info-list__value'}))
    try:
        info_list = {key.text: value.text.strip() for key, value in zip(keys, values)}
    except:
        info_list = {}

    statistics_div = (soup.find('div', {'class': 'media-section_stats'}).
                      find_all('div', {'class': 'media-section__col'}))

    try:
        stats = {key: int(value.text) for key, value in
                 zip(lists, statistics_div[0].find_all('div', {'class': 'media-stats-item__count'}))}
    except IndexError:
        stats = {}

    try:
        ratings = {key: int(value.text.strip()) for key, value in
                   zip(stars, statistics_div[1].find_all('div', {'class': 'media-stats-item__count'}))}
    except IndexError:
        ratings = {}

    return tags, info_list, stats, ratings, alt_name, name


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
            print('Ошибка при поиске любимых тайтлов')
        elif '?folder=3' in user_url:
            print('Ошибка при поиске брошеных тайтлов')
        print(e)

    return links


def parse_catalog(driver, links):
    n_page = 1
    max_scrolls = 100
    zero_streak = 0

    while zero_streak < 5:
        d_link = f'https://mangalib.me/manga-list?sort=rate&dir=desc&page={n_page}&site_id=1'
        driver.get(d_link)
        scroll_count = 0
        prev_height = -1
        try:
            while scroll_count < max_scrolls:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                WebDriverWait(driver, 240).until_not(
                    EC.presence_of_element_located((By.CLASS_NAME, 'manga-block-items_loading'))
                )
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == prev_height:
                    break
                prev_height = new_height
                scroll_count += 1
        except Exception as error:
            print(error)
            print('Вышло время ожидания')

        soup = BeautifulSoup(driver.page_source)
        manga_cards = soup.find_all('div', {'class': "media-card-wrap"})
        new_links = 0
        for manga_card in manga_cards:
            link = str(manga_card.find('a').get('href'))
            if link not in links:
                new_links += 1
                links.add(link)

        if new_links:
            print(f'Новых ссылок: {new_links}')
            print(f'страница: {n_page}')
            n_page += 1
            zero_streak = 0
        else:
            print(f'Новых ссылок: {new_links}')
            print(f'страница: {n_page}')
            zero_streak += 1
            n_page += 1
    return links
