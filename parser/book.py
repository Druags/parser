from files import upload_result, get_links, get_used_links_csv

from parsers import parse_books, authorize


class Book:
    def __init__(self, name, link):
        self.type = ''
        self.name = name
        self.alt_name = self.name
        self.link = link
        self.tags = []
        self.info_list = {}
        self.stats = {}
        self.ratings = {}

    def print_info(self):
        print(f'Тип: {self.type}, \n'
              f'Имя: {self.name}, \n'
              f'Ссылка: {self.link}, \n'
              f'Тэги: {", ".join([tag for tag in self.tags]) if self.tags else ""} \n')

    def set_attrs(self, tags, info_list, stats, ratings, alt_name, name):
        self.set_tags(tags)
        self.set_alt_name(alt_name)
        self.set_info_list(info_list)
        self.set_stats(stats)
        self.set_type()
        self.set_ratings(ratings)
        self.name = name

    def set_tags(self, tags):
        self.tags = tags

    def set_alt_name(self, alt_name):
        self.alt_name = alt_name

    def set_info_list(self, info_list):
        self.info_list = info_list

    def set_stats(self, stats):
        self.stats = stats

    def set_type(self):
        self.type = self.info_list['Тип']

    def set_ratings(self, ratings):
        self.ratings = ratings

    def get_info(self):
        return self.__dict__


def create_book_objects(file_name):
    links = get_links(file_name)
    books = []
    for link in links:
        books.append(Book(name='name', link=link))
    return books


def create_books_data(driver, read_file_name='unique_title_links', write_file_name='manga_data', auth=False):
    if auth:
        authorize(driver)

    write_file_name = f'../data/{write_file_name}.csv'
    read_file_name = f'../data/{read_file_name}.csv'
    used_links = get_used_links_csv(write_file_name, link_col=3)
    book_objects = create_book_objects(read_file_name)
    book_links = [book for book in book_objects if book.link not in used_links]
    parse_books(driver, book_links, write_file_name)
    # upload_result(write_file_name)
