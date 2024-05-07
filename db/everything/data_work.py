from typing import Any
import numpy as np
import pandas as pd

from config import DATA_DIR


def change_sex(sex: str) -> int:
    if sex == 'Unknown':
        return 0
    elif sex == 'Мужской':
        return 1
    elif sex == 'Женский':
        return 2


def get_url_last_part(url) -> str:
    return url.split('/')[-1]


def get_field_values(info_list: pd.Series, field_name: str) -> pd.Series:
    field_values = []
    for element in info_list:
        element = eval(element)
        field_values.append(element.get(field_name))
    return pd.Series(field_values)


def get_unique_values() -> None:
    data = pd.read_csv(DATA_DIR + 'manga_data_expanded.csv')
    cols = ['authors', 'artists', 'publishers']
    authors = set()
    artists = set()
    publishers = set()
    for t in data[cols].itertuples():
        authors.update(t.authors.split('\n'))
        artists.update(t.artists.split('\n'))
        publishers.update(t.publishers.split('\n'))

    pd.Series(list(authors)).to_csv(DATA_DIR + 'authors.csv', encoding='UTF-8', index=False, header=False)
    pd.Series(list(artists)).to_csv(DATA_DIR + 'artists.csv', encoding='UTF-8', index=False, header=False)
    pd.Series(list(publishers)).to_csv(DATA_DIR + 'publishers.csv', encoding='UTF-8', index=False,
                                       header=False)


def remove_duplicates_from_list(data: pd.Series) -> list:
    result = []
    for value in data.values:
        if value:
            if '[' in value and ']' in value:
                for sym in '[', ']', "'":
                    value = value.replace(sym, '')
                value = set(value.split(', '))
                result.append(set(map(lambda x: x.lower(), value)))
            else:
                result.append(set(map(lambda x: x.lower(), value.split('\n'))))
        else:
            result.append(set())
    return result


def fix_year(year: str) -> str:
    if year and len(year) > 4:
        return year[:4]
    else:
        return year


def get_user_info(file_path: str) -> pd.DataFrame:
    user_info = pd.read_csv(file_path)
    user_info = user_info.replace(np.nan, None)
    user_info['favorite_tags'] = remove_duplicates_from_list(user_info['favorite_tags'])
    user_info['favorite_titles'] = remove_duplicates_from_list(user_info['favorite_titles'])
    user_info['abandoned_titles'] = remove_duplicates_from_list(user_info['abandoned_titles'])

    return user_info


def get_title_info(file_path: str) -> pd.DataFrame:
    title_info = pd.read_csv(file_path)
    title_info = title_info.replace(np.nan, None)
    cols = ['in_lists', 'ratings', 'authors', 'artists', 'release_formats', 'tags', 'publishers']
    for col in cols:
        title_info[col] = title_info[col].apply(eval)

    return title_info


def expand_manga_data(regime: str = 'return') -> Any:
    data = pd.read_csv(DATA_DIR + 'manga_data.csv')

    cols = ["Год релиза", "Статус тайтла", 'Статус перевода',
            'Автор', 'Художник', "Издательство", 'Загружено глав', "Возрастной рейтинг", "Формат выпуска"]
    info_list = data['info_list']
    for col in cols:
        data[col] = get_field_values(info_list, col)
    data['link'] = data['link'].apply(get_url_last_part)
    data = data.rename(columns={'Тип': 'type',
                                'Год релиза': 'release_year',
                                'Статус тайтла': 'publication_status',
                                'Статус перевода': 'translation_status',
                                'Автор': 'authors',
                                'Художник': 'artists',
                                'Издательство': 'publishers',
                                'Загружено глав': 'chapters_uploaded',
                                'Возрастной рейтинг': 'age_rating',
                                'link': 'url',
                                'Формат выпуска': 'release_formats',
                                'stats': 'in_lists'})
    data = data.drop_duplicates(subset=['url'])
    data['in_lists'] = data['in_lists'].apply(eval)
    data['ratings'] = data['ratings'].apply(eval)

    data.drop(columns=['alt_name', 'info_list'], inplace=True)

    m2m_cols = ['publishers', 'authors', 'artists', 'release_formats', 'tags']
    data = data.replace(np.nan, None)
    for m2m_col in m2m_cols:
        data[m2m_col] = remove_duplicates_from_list(data[m2m_col])
    data['release_year'] = data['release_year'].apply(fix_year)
    if regime == 'return':
        return data
    else:
        data.to_csv(DATA_DIR + 'manga_data_expanded.csv', encoding='UTF-8', index=False)


def get_favorite_titles() -> pd.DataFrame:
    columns = ['user_url', 'title_url']
    data = pd.read_csv(DATA_DIR + 'favorite_titles.csv',
                       header=None,
                       names=columns,
                       dtype='str')
    return data


def merge_user_and_titles() -> None:
    user_info_data = pd.read_csv(DATA_DIR + 'users_info.csv', names=['url', 'sex', 'favorite_genres'])
    fav_title_data = pd.read_csv(DATA_DIR + 'favorite_titles.csv', names=['user_url', 'title_url'])
    ab_title_data = pd.read_csv(DATA_DIR + 'abandoned_titles.csv', names=['user_url', 'title_url'])

    fav_title_data['title_url'] = fav_title_data['title_url'].apply(get_url_last_part)
    ab_title_data['title_url'] = ab_title_data['title_url'].apply(get_url_last_part)

    fav_titles = dict()
    for row in fav_title_data.itertuples():
        fav_titles.setdefault(row.user_url, []).append(row.title_url)
    fav_titles = pd.Series(fav_titles, name='title_url')

    ab_titles = dict()
    for row in ab_title_data.itertuples():
        ab_titles.setdefault(row.user_url, []).append(row.title_url)
    ab_titles = pd.Series(ab_titles, name='title_url')

    user_info_data = pd.merge(user_info_data, fav_titles, left_on='url', right_index=True)
    user_info_data = pd.merge(user_info_data, ab_titles, left_on='url', right_index=True)

    user_info_data.rename(columns={'title_url_x': 'favorite_titles',
                                   'title_url_y': 'abandoned_titles'},
                          inplace=True)
    user_info_data['url'] = user_info_data['url'].apply(get_url_last_part)
    user_info_data['sex'] = user_info_data['sex'].apply(change_sex)
    user_info_data.to_csv(DATA_DIR + 'users_full_info.csv',
                          index=False)
