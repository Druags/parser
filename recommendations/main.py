import pandas as pd
import sys
import os
from IPython.display import display

from help_functions import get_avg_rating, get_total_reviews, set_names, get_field_values, clean_year


sys.path.insert(1, os.path.join(sys.path[0], '..'))

users_data = pd.read_csv('data/users_info.csv', header=None, dtype='str')
columns = ['url', 'sex', 'favorite_genres', 'favorite_titles', 'abandoned_titles']
users_data.rename(columns={old: new for old, new in zip(users_data.columns, columns)}, inplace=True)

titles_data = pd.read_csv('.data/manga_data.csv')
titles_data.fillna('nothing')
titles_data['avg_rating'] = titles_data['ratings'].apply(get_avg_rating)
titles_data['total_reviews'] = titles_data['ratings'].apply(get_total_reviews)
titles_data['tags'] = titles_data['tags'].apply(eval)
titles_data['name'] = set_names(titles_data['name'],
                                titles_data['alt_name'])
titles_data['author'] = get_field_values(titles_data['info_list'],
                                         'Автор')
titles_data['type'] = titles_data['type'].astype('category')
titles_data['year'] = get_field_values(titles_data['info_list'],
                                       'Год релиза').fillna('0').apply(clean_year).astype('int')
titles_data['status'] = get_field_values(titles_data['info_list'],
                                         'Статус тайтла').astype('category')

favorite_titles = []
for i, user in enumerate(users_data['url']):
    for title_url in eval(users_data.loc[i, 'favorite_titles']):
        favorite_titles.append([user, title_url])
favorite_titles = pd.DataFrame(favorite_titles, columns=['user_url', 'title_url'])
users_data.drop(columns=['favorite_titles'], inplace=True)

abandoned_titles = []
for i, user in enumerate(users_data['url']):
    for title_url in eval(users_data.loc[i, 'abandoned_titles']):
        abandoned_titles.append([user, title_url])
abandoned_titles = pd.DataFrame(abandoned_titles, columns=['user_url', 'title_url'])
users_data.drop(columns=['abandoned_titles'], inplace=True)

title_tags = []
for i, url in enumerate(titles_data['link']):
    tags = titles_data.loc[i, 'tags']
    for tag in tags:
        title_tags.append((url, tag))
title_tags = pd.DataFrame(title_tags, columns=['title_url', 'tag'])

my_favs = ['https://mangalib.me/ajin',
           'https://mangalib.me/dungeon-meshi',
           'https://mangalib.me/gantz',
           'https://mangalib.me/kingdom',
           'https://mangalib.me/shuumatsu-no-walkure-shuumatsu-no-valkyrie',
           'https://mangalib.me/berserk',
           'https://mangalib.me/chainsaw-man',
           'https://mangalib.me/chainsaw-man-2',
           'https://mangalib.me/dwaejiuri-',
           'https://mangalib.me/goodbye-eri',
           'https://mangalib.me/gyakusatsu-happiendo',
           'https://mangalib.me/hunter_x_hunter',
           'https://mangalib.me/listen-to-futu',
           'https://mangalib.me/jumyou-wo-kaitotte-moratta-ichinen-ni-tsuki-ichimanen-de',
           'https://mangalib.me/one-piece',
           'https://mangalib.me/sakamoto-deizu',
           'https://mangalib.me/vinland-saga',
           'https://mangalib.me/yakusoku-no-neverland']

good_users = []
for my_fav in my_favs:
    good_users.append(favorite_titles[favorite_titles['title_url'] == my_fav]['user_url'])

good_users_count = {}
for group in good_users:
    for user in group:
        if good_users_count.get(user):
            good_users_count[user] += 1
        else:
            good_users_count[user] = 1

good_users = pd.DataFrame(good_users_count.items(), columns=['user_url', 'intersections'])
users_title_count = favorite_titles[favorite_titles['user_url'].isin(good_users['user_url'])].groupby(
    'user_url').count()
good_users = good_users.merge(users_title_count, how='inner', on='user_url').rename(columns={'title_url': 'n_titles'})

good_users['ratio'] = good_users['intersections'] / good_users['n_titles']

ignored_tags = []
fields_to_show = ['author', 'type', 'year', 'name', 'link', 'avg_rating', 'total_reviews', 'status']
ignore_list = title_tags[title_tags['tag'].isin(ignored_tags)]['title_url'].unique()

best_users = good_users[good_users['n_titles'] > 5].sort_values('ratio', ascending=False).head(15)
best_users = best_users.merge(favorite_titles, on='user_url')
best_users = best_users[~best_users['title_url'].isin(my_favs)]
recommended_titles = best_users.groupby('title_url').agg(cnt=('ratio', 'count')).sort_values('cnt',
                                                                                             ascending=False).head(20)
recommendations = titles_data[(titles_data['link'].isin(recommended_titles.index))][fields_to_show]
recommendations = recommendations.merge(recommended_titles.cnt, left_on='link', right_on='title_url')

display(recommendations.sort_values(by=['cnt'], ascending=False))
