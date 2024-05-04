from bs4 import BeautifulSoup


def get_user_sex(soup: BeautifulSoup) -> str:
    try:
        page_modals = soup.find('div', {'class': 'page-modals'})
        profile_info = page_modals.find('table', {'class': 'profile-info'})
        info_row = profile_info.find_all('tr', {'class': 'profile-info__row'})[1]
        sex = info_row.find('td', {'class': 'profile-info__value'}).text
    except Exception as e:
        sex = 'Unknown'
        print('Ошибка при поиске пола пользователя')
        print(e)
    return sex


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

def check_user_account(driver, user_link):


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

def user_is_active(soup) -> bool:
    try:
        bookmark_menu = soup.find('div', {'class': 'bookmark-menu'})
        bookmarks = bookmark_menu.find_all('div', {'class': 'menu__item'})
        books_n = int(bookmarks[0].find('span', {'class': 'bookmark-menu__label'}).text)
        books_favorite_n = int(bookmarks[5].find('span', {'class': 'bookmark-menu__label'}).text)

        if not (books_favorite_n == 0 or books_n < 50):
            return True
    except Exception as error:
        print('User inactive', error)

