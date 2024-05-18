import os
import pprint
import signal
import time
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.everything.models import Base, UserORM, TitleORM, TypeORM
from db.everything.queries import add_full_users
from parsers.data_classes import UserIsInactiveException, PageNotFound
from parsers.code import start_driver, get_users_info, get_books_info

import pandas as pd

import data
from parsers.parsers_funcs import parse_user_info, parse_title


class TestQueries(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.start_time = time.time()
        self.driver = start_driver()
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(bind=self.engine)
        self.session_factory = sessionmaker(bind=self.engine)

    def tearDown(self):
        with self.session_factory() as session:
            Base.metadata.drop_all(bind=self.engine)
            session.close()
        t = time.time() - self.start_time

        pid = self.driver.service.process.pid
        self.driver.close()

        try:
            os.kill(int(pid), signal.SIGTERM)
            print("Killed chrome using process")
        except ProcessLookupError as ex:
            pass
        print('%s: %.3f' % (self.id(), t))

    def test_positive_parse_user_info(self):
        url = 'https://mangalib.me/user/919160'
        result = parse_user_info(self.driver, url)

        self.assertEqual(data.expectation, result)

    def test_positive_get_users_info_test(self):
        user_links = [919160]
        get_users_info(self.driver, self.session_factory, user_links)

        with self.session_factory() as session:
            result = session.query(UserORM).first().url

        self.assertEqual(user_links[0], result)

    def test_negative_get_users_info_test_by_last_id(self):
        get_users_info(self.driver, self.session_factory)

        self.assertRaises(UserIsInactiveException)

    def test_negative_get_users_info_test(self):
        get_users_info(self.driver, self.session_factory, [0])

        self.assertRaises(PageNotFound)

    def test_positive_get_users_info_test_by_last_id(self):
        users_info = pd.DataFrame({'url': [919159],
                                   'sex': [1],
                                   'favorite_tags': [{'1'}],
                                   'favorite_titles': [{'1'}],
                                   'abandoned_titles': [{'1'}]})
        add_full_users(self.session_factory, users_info)
        get_users_info(self.driver, self.session_factory)

        with self.session_factory() as session:
            result = session.query(UserORM).order_by(UserORM.id.desc()).first()
            user_id = result.id
            user_sex = result.sex
            user_f_titles = len(result.favorite_titles)
            user_a_titles = len(result.abandoned_titles)

        self.assertCountEqual([2, 2, 17, 20],
                              [user_id, user_sex, user_f_titles, user_a_titles])

    def test_positive_get_users_info_test_by_last_id_with_amount(self):
        users_info = pd.DataFrame({'url': [919160],
                                   'sex': [1],
                                   'favorite_tags': [{'1'}],
                                   'favorite_titles': [{'1'}],
                                   'abandoned_titles': [{'1'}]})
        add_full_users(self.session_factory, users_info)
        get_users_info(self.driver, self.session_factory, amount=100)

        with self.session_factory() as session:
            result = session.query(UserORM).all()
            print(result)

    def test_unknown_error(self):
        get_users_info(self.driver, self.session_factory, user_ids=[919215])
        self.assertRaises(AttributeError)

    def test_positive_parse_book(self):
        book_url = 'ushiro-no-shoumen-kamui-san'
        result = parse_title(self.driver,
                            'https://mangalib.me/' + book_url)
        self.assertCountEqual(result, data.book_example)

    def test_positive_parse_books_test(self):
        get_books_info(self.driver, self.session_factory)

        with self.session_factory() as session:
            result = session.query(TitleORM).first()

        self.assertEqual('<TitleORM id=1, url=ushiro-no-shoumen-kamui-san, release_year=2020>',
                         str(result))

    def test_positive_parse_books_test_db(self):
        with self.session_factory() as session:
            session.add(TitleORM(url='ushiro-no-shoumen-kamui-san'))
            session.add(TypeORM(name='test_type'))
            session.flush()
            session.add(TitleORM(url='test_2', type=1))
            session.flush()
            session.commit()

        get_books_info(self.driver, self.session_factory, use_db=True)

        with self.session_factory() as session:
            result = session.query(TitleORM).first()

        self.assertEqual('<TitleORM id=1, url=ushiro-no-shoumen-kamui-san, release_year=2020>',
                         str(result))

    def test_negative_parse_book(self):
        url = 'https://mangalib.me/63067'
        parse_title(self.driver, url)
        self.assertRaises(AttributeError)

    def test_positive_parse_book_new(self):
        url = 'https://mangalib.me/i-played-the-role-of-a-hated-hero-but-for-some-reason-im-loved-by-the-last-boss-and-living-with-her'
        result = parse_title(self.driver, url)
        expected = {'type': 'Манга', 'tags': {'боевик', 'гарем', 'комедия', 'магия', 'фэнтези', 'гг мужчина', 'этти',
                                              'волшебники / маги', 'приключения'},
                    'ratings': {10: 141, 9: 10, 8: 38, 7: 18, 6: 24, 5: 10, 4: 6, 3: 5, 2: 4, 1: 32},
                    'release_year': '2021', 'publication_status': 'Завершён', 'translation_status': 'Завершен',
                    'authors': {'sadakiyo kazuhiko'}, 'artists': {'sadakiyo kazuhiko'}, 'publishers': {'takeshobo'},
                    'chapters_uploaded': '22', 'age_rating': '18+', 'release_formats': {'веб'}}

        self.assertEqual(expected, result)

    def test_parse_book_cant_click_button(self):
        url = 'https://mangalib.me/salvos-webtoon'
        result = parse_title(self.driver, url)


