import pandas as pd

data = pd.read_csv('data/manga_data_expanded.csv')

print(data['type'].unique())
