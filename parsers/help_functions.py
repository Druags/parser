from bs4 import BeautifulSoup


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
        print('Пол не указан')
    except AttributeError:
        sex = 2
        print('Пол не указан, но есть описание')
    except Exception as e:
        sex = 2
        print('Неизвестная ошибка при поиске пола', e)
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

