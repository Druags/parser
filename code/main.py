import csv
from selenium import webdriver
import os
import time
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager

from book import create_books_data
from files import get_links, write_to_csv, get_used_links_csv
from parsers import parse_catalog, parse_book, check_user_account, parse_user_info, parse_user_mangas
import modes


def get_active_users(driver, start_id, end_id):
    links = 0
    with open(f'../data/user_links.csv', 'a', encoding='UTF-8', newline='') as file:
        csv_writer = csv.writer(file)
        for user_id in range(start_id, end_id):
            user_link = f'https://mangalib.me/user/{user_id}'
            if check_user_account(driver, user_link):
                csv_writer.writerow([user_link])
                links += 1
        print(f'Заполненных аккаунтов: {links}')


def get_catalog(driver, file_name):
    links = get_links(file_name)
    start_len_links = len(links)
    links = parse_catalog(driver, links)
    if len(links) != start_len_links:
        write_to_csv(file_name, links)


def get_user_mangas(driver):
    user_links = [

                  'https://mangalib.me/user/262?folder=all',
                  'https://mangalib.me/user/3732?folder=all',
                  'https://mangalib.me/user/55643?folder=all']
    result_links = set(get_links('unique_title_links'))
    with open(f'../data/unique_title_links.csv', 'a', encoding='UTF-8', newline='') as file:
        csv_writer = csv.writer(file)
        for user_link in user_links:
            catalog = parse_user_mangas(driver, user_link)
            new_links = catalog - result_links
            catalog.update(new_links)
            print(f'new links: {len(new_links)}')
            for new_link in new_links:
                csv_writer.writerow([new_link])


def get_users_info(driver, user_links_file='../data/user_links.csv', output_file='../data/users_info.csv'):
    all_user_links = get_links(user_links_file)
    used_links = get_used_links_csv(output_file, link_col=0)
    links = [link for link in all_user_links if link not in used_links]
    with open(output_file, 'a', encoding='UTF-8', newline='') as file,\
            open('../backup/favorite_titles.csv', 'a', newline='') as favs,\
            open('../data/abandoned_titles.csv', 'a',  newline='') as aban:

        csv_writer_main = csv.writer(file)
        csv_writer_favs = csv.writer(favs)
        csv_writer_aban = csv.writer(aban)
        for i, user_link in enumerate(links):
            # TODO каждые 70 крашится из-за нехватки памяти
            if i % 70 == 0:
                driver.quit()
                driver = start_driver()
            try:
                sex, genres_amount, fav_titles, aban_titles = parse_user_info(driver, user_link)
                csv_writer_main.writerow([user_link, sex, genres_amount])
                csv_writer_favs.writerows([(user_link, title) for title in fav_titles])
                csv_writer_aban.writerows([(user_link, title) for title in aban_titles])
            except Exception as e:
                print(user_link, e)


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
            driver_executable_path=ChromeDriverManager().install()
        )
    else:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--blink-settings=imagesEnabled=false')
        chrome_options.add_argument("--disable-cache")
        driver = webdriver.Chrome(chrome_options)
    return driver


def main(mode=None):
    driver = start_driver()

    if not (os.path.exists('data') and os.path.isdir('data')):
        os.mkdir('data')

    book_link = 'https://mangalib.me/penguin-peeves'
    file_name = '../data/title_links.csv'
    user_link = 'https://mangalib.me/user/565777'

    if mode == modes.CATALOG:
        get_catalog(driver, file_name)
    elif mode == modes.USER_MANGAS:
        get_user_mangas(driver)
    elif mode == modes.PARSE_BOOK:
        values = parse_book(driver, book_link)
    elif mode == modes.USER_STATUS:
        get_active_users(driver, start_id=125850, end_id=130000)
    elif mode == modes.USER_INFO:
        get_users_info(driver)
    elif mode == modes.CREATE_BOOKS_DATA:
        create_books_data(driver, read_file_name='failures', auth=True)
    # driver.get_screenshot_as_file('final.png')
    driver.close()


if __name__ == '__main__':
    mode = modes.USER_INFO
    start_time = time.time()
    main(mode)

    end_time = time.time()
    work_time = end_time - start_time

    print(f'Время работы программы: {int(work_time // 3600 )}:{int(work_time // 60 % 60)}:{round(work_time % 60, 2)}')

