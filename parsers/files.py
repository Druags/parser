import csv
import json
import os

import pandas as pd

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


def save_books_csv(books, file_name):

    if os.path.exists(f'../data/{file_name}.csv'):
        books_info = [book.get_info() for book in books]
        write_to_csv(file_name, books_info, element_type='dict')
    else:
        file_name = f'../data/{file_name}.csv'
        df = pd.DataFrame.from_records([book.get_info() for book in books])
        df.to_csv(file_name, index=False)


def get_links(file_path):
    links = []
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='UTF-8') as file:
            csv_reader = csv.reader(file)
            for line in csv_reader:
                links.append(line[0])
    else:
        print(f'Файла {file_path} не существует')
    return links


def write_to_csv(file_name, links, element_type='str'):
    file_name = f'../data/{file_name}.csv'
    with open(file_name, 'a', newline='', encoding='UTF-8') as file:
        csv_writer = csv.writer(file)
        if element_type == 'str':
            for link in links:
                csv_writer.writerow([link])
        elif element_type == 'dict':
            for link in links:
                csv_writer.writerow([*link.values()])


def get_used_links_csv(file_path, link_col):
    used_links = []
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                used_links.append(row[link_col])
    return used_links


def get_used_links_json(file_name):
    file_name = f'../data/{file_name}.json'
    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8') as file:
            return json.loads(file.read())


def upload_result(file_name):  # todo переделать для проверки на существование файла
    g_login = GoogleAuth()
    g_login.LocalWebserverAuth()
    drive = GoogleDrive(g_login)
    file_name = '../data/' + file_name
    try:
        f = drive.CreateFile({
            'title': f'{file_name.split("/")[-1]}.csv',
            'id': "1eOfnUIf-itqdqxn3QWO7C86xE9w2dOT-",
            "parents": [{"id": '17ZtO-XJs4PNZoCmUMODd6zzWxr9BcFaz'}]
        })
        f.SetContentFile(f'{file_name}.csv')
    except FileNotFoundError:
        print(1)
        f = drive.CreateFile({
            'title': f'{file_name.split("/")[-1]}.csv',
            "parents": [{"id": '17ZtO-XJs4PNZoCmUMODd6zzWxr9BcFaz'}]
        })
        f.SetContentFile(f'{file_name}.csv')
    f.Upload()
