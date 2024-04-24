import pandas as pd

from config import DATA_DIR


title_types = pd.DataFrame(['Манхва', 'Манга', 'Маньхуа', 'Комикс западный', 'OEL-манга', 'Руманга'],
                           columns=['name'])
publication_statuses = pd.DataFrame(['Завершён', 'Онгоинг', 'Выпуск прекращён', 'Приостановлен', 'Анонс'],
                                    columns=['name'])
age_ratings = pd.DataFrame(['16+', '18+'], columns=['name'])
translation_statuses = pd.DataFrame(['Завершен', 'Заброшен', 'Продолжается', 'Заморожен'],
                                    columns=['name'])
release_formats = pd.DataFrame(['4-кома (Ёнкома)', 'В цвете', 'Веб', 'Вебтун', 'Додзинси', 'Сборник', 'Сингл'],
                               columns=['name'])
authors = pd.read_csv(DATA_DIR + 'authors.csv', names=['name'])
artists = pd.read_csv(DATA_DIR + 'artists.csv', names=['name'])
publishers = pd.read_csv(DATA_DIR + 'publishers.csv', names=['name'])
tags = pd.read_csv(DATA_DIR + 'tags.csv', names=['name'])