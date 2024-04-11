import csv
import sys
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager

# optional
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

start_time = time.time()

index_start = int(sys.argv[1].split('-')[0])
index_end = int(sys.argv[1].split('-')[1])

# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--blink-settings=imagesEnabled=false')
# driver = webdriver.Chrome(chrome_options)

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

good_links = []
with (open(f'good_links_{sys.argv[1]}.csv', 'a', newline='') as file,
      open(f'errors_{sys.argv[1]}.csv', 'a', newline='') as errors):
    csv_writer = csv.writer(file)
    csv_error_writer = csv.writer(errors)
    for user_id in tqdm(range(index_start, index_end)):
        try:
            user_link = f'https://mangalib.me/user/{user_id}'
            driver.get(user_link)
            if '404' not in driver.title:
                WebDriverWait(driver, 20).until_not(
                    EC.presence_of_element_located((By.CLASS_NAME, 'loader-wrapper'))
                )
                soup = BeautifulSoup(driver.page_source)
                bookmarks = (soup.find('div', {'class': 'bookmark-menu'}).
                             find_all('div', {'class': 'menu__item'}))
                if not (int(bookmarks[5].find('span', {'class': 'bookmark-menu__label'}).text) == 0 or
                        int(bookmarks[0].find('span', {'class': 'bookmark-menu__label'}).text) < 50):
                    csv_writer.writerow([user_link])
        except Exception as e:
            csv_error_writer.writerow([f'https://mangalib.me/user/{user_id}'])
    end_time = time.time()
