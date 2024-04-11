import csv


# import undetected_chromedriver as uc
# from webdriver_manager.chrome import ChromeDriverManager


def unique_rows(file_path='../data/favorite_titles.csv', read=True, write=False):
    if read:
        with open(file_path, 'r', encoding='UTF-8') as file:
            data_read = csv.reader(file)
            unique_rows = set()
            for row in data_read:
                unique_rows.add(str(row))
        print(len(unique_rows))
    if write:
        new_path = file_path.split('.csv')[0] + '_unique.csv'
        with open(new_path, 'w', encoding='UTF-8', newline='') as file:
            csv_writer = csv.writer(file)
            for row in unique_rows:
                csv_writer.writerow(eval(row))


def get_data():
    with open('data/manga_data.csv', 'r', encoding='UTF-8') as file, open('data/manga_data_test.csv', 'w',
                                                                          encoding='UTF-8',
                                                                          newline='') as write_file:
        data_read = csv.reader(file)
        data_write = csv.writer(write_file)
        books = set()
        for row in data_read:
            link = row[3].split('/')[-1]
            print(row[0])
            if link not in books and row[0]:
                books.add(link)
                data_write.writerow(row)

    # with open('data/manga_data.json', 'w', encoding='UTF-8') as file:
    #     json.dump(books, file, ensure_ascii=False, indent=3)
    print(len(books))


def connect_csv_cols(file_name):
    with open(file_name, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        result = set()
        for row in csv_reader:
            print(''.join(row))
            result.add(''.join(row))
    with open(file_name, 'w', encoding='utf-8', newline='') as out_file:
        csv_writer = csv.writer(out_file)
        for row in result:
            csv_writer.writerow([row])


def delete_ids():
    with open('data/manga_data.csv', 'r', encoding='UTF-8') as file:
        data_read = csv.reader(file)

        with open('data/test_manga_data.csv', 'w', encoding='UTF-8', newline='') as file:
            data_write = csv.writer(file)
            for row in data_read:
                data_write.writerow(row[1:])


# def test_cloudflare():
#     options = Options()
#     options.add_argument("--headless")  # Headless mode
#
#     # driver = webdriver.Chrome(options=options)
#
#     driver = uc.Chrome(ChromeDriverManager().install())
#     driver.get("https://mangalib.me/")
#
#     time.sleep(20)
#
#     driver.save_screenshot("mangalib.png")
#
#     driver.close()
#
#
# test_cloudflare()

with open('../backup/users_info.csv', 'r', encoding='UTF-8') as i_file, \
        open('../data/users_info.csv', 'a', encoding='UTF-8', newline='') as o_file, \
        open('../backup/favorite_titles.csv', 'a', encoding='UTF-8', newline='') as favs, \
        open('../data/abandoned_titles.csv', 'a', encoding='UTF-8', newline='') as aban:
    csv_reader = csv.reader(i_file)
    csv_writer_users = csv.writer(o_file)
    csv_writer_favs = csv.writer(favs)
    csv_writer_aban = csv.writer(aban)
    for row in csv_reader:
        url, sex, genres, favorites, abandoned = row
        csv_writer_users.writerow([url, sex, genres])
        # csv_writer_favs.writerows([[url, title] for title in eval(favorites)])
        # csv_writer_aban.writerows([[url, title] for title in eval(abandoned)])
