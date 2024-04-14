import csv
import os
import sys
import pandas as pd

sys.path.insert(1, os.path.join(sys.path[0], '..'))


# TODO унифицировтаь под похожие случаи
def get_authors() -> pd.DataFrame:
    data = pd.read_csv('../../data/manga_data_expanded.csv')
    result = []
    for row in data[['url', 'authors']].itertuples():
        for author in row.authors.split('\n'):
            result.append((row.url, author))
    result = pd.DataFrame(result, columns=['title_url', 'author_name'])

    return result


def change_sex(sex: str) -> int:
    if sex == 'Unknown':
        return 0
    elif sex == 'Мужской':
        return 1
    elif sex == 'Женский':
        return 2


def get_titles_url() -> pd.DataFrame:
    unique_titles = set()
    file_names = ['../../data/favorite_titles.csv', '../../data/abandoned_titles.csv']
    for file_name in file_names:
        with open(file_name) as file:
            csv_reader = csv.reader(file)
            for line in csv_reader:
                unique_titles.add(line[1].split('/')[-1])
    titles_df = pd.read_csv('../../data/manga_data_expanded.csv')
    titles_urls = list(titles_df['url'])
    unique_titles.update(titles_urls)
    unique_titles = pd.DataFrame(unique_titles, columns=['url'])

    return unique_titles


def get_url_last_part(url) -> str:
    return url.split('/')[-1]


def get_pairs(file_name: str) -> pd.DataFrame:
    df = pd.read_csv(file_name, header=None,
                     names=['user_id', 'title_url'],
                     dtype='str').drop_duplicates()
    df['user_id'] = df['user_id'].apply(get_url_last_part).astype(int)
    df['title_url'] = df['title_url'].apply(get_url_last_part)
    return df


def get_user_data() -> pd.DataFrame:
    columns = ['url', 'sex', 'favorite_genres']
    data = pd.read_csv('../../data/users_info.csv',
                       header=None,
                       names=columns,
                       dtype='str').drop_duplicates(subset=['url'])
    data['sex'] = data['sex'].apply(change_sex)
    data['url'] = data['url'].apply(get_url_last_part)
    data.rename(columns={'url': 'id'}, inplace=True)

    return data[['id', 'sex']]


def get_field_values(info_list: pd.Series, field_name: str) -> pd.Series:
    field_values = []
    for element in info_list:
        element = eval(element)
        field_values.append(element.get(field_name))
    return pd.Series(field_values)


def set_name(old_names: pd.Series, new_names: pd.Series) -> list:
    names = []
    for old_name, new_name in zip(old_names, new_names):
        if old_name == 'name':
            names.append(new_name)
        else:
            names.append(old_name)

    return names


def get_unique_values() -> None:
    data = pd.read_csv('../data/manga_data_expanded.csv')
    cols = ['authors', 'artists', 'publishers']
    authors = set()
    artists = set()
    publishers = set()
    for t in data[cols].itertuples():
        authors.update(t.authors.split('\n'))
        artists.update(t.artists.split('\n'))
        publishers.update(t.publishers.split('\n'))

    pd.Series(list(authors)).to_csv('../data/authors.csv', encoding='UTF-8', index=False, header=False)
    pd.Series(list(artists)).to_csv('../data/artists.csv', encoding='UTF-8', index=False, header=False)
    pd.Series(list(publishers)).to_csv('../data/publishers.csv', encoding='UTF-8', index=False, header=False)


def expand_manga_data() -> None:
    data = pd.read_csv('../../data/manga_data.csv')
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
                                'Формат выпуска': 'release_format'})
    data['name'] = set_name(data['name'], data['alt_name'])
    data.drop(columns=['alt_name', 'info_list'], inplace=True)
    # na_cols = ['publication_status', 'translation_status',
    #            'authors', 'artists', 'publishers', 'chapters_uploaded', 'age_rating', 'year']
    # for na_col in na_cols:
    #     data[na_col] = data[na_col].fillna('Unknown')

    data.to_csv('../../data/manga_data_expanded.csv', encoding='UTF-8', index=False)



def get_abandoned_titles() -> pd.DataFrame:
    columns = ['user_url', 'title_url']
    data = pd.read_csv('../data/abandoned_titles.csv',
                       header=None,
                       names=columns,
                       dtype='str')
    return data


def get_favorite_titles() -> pd.DataFrame:
    columns = ['user_url', 'title_url']
    data = pd.read_csv('../backup/favorite_titles.csv',
                       header=None,
                       names=columns,
                       dtype='str')
    return data


def save_csv(data: dict) -> None:
    file_name = f'data/users_and_titles.csv'

    df = pd.DataFrame.from_records(data,
                                   columns=['user_id', 'user_link', 'title_id', 'title_link'])
    df.to_csv(file_name, index=False)

