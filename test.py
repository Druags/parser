import pandas as pd

data = pd.read_csv('data/manga_data_expanded.csv')

for authors in data['authors']:
    print('|')
    for author in authors.split('\n'):
        print(f'| | {author}')
