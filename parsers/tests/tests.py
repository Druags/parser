import time
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.everything.models import Base, UserORM
from db.everything.queries import add_full_users
from parsers.data_classes import UserIsInactiveException, PageNotFound
from parsers.main import start_driver, get_users_info_test

import pandas as pd

import data
from parsers.parsers_funcs import parse_user_info


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
        self.driver.quit()
        print('%s: %.3f' % (self.id(), t))

    def test_positive_parse_user_info(self):
        url = 'https://mangalib.me/user/919160'
        result = parse_user_info(self.driver, url)

        self.assertEqual(data.expectation, result)

    def test_positive_get_users_info_test(self):
        user_links = [919160]
        get_users_info_test(self.driver, self.session_factory, user_links)

        with self.session_factory() as session:
            result = session.query(UserORM).first().url

        self.assertEqual(user_links[0], result)

    def test_negative_get_users_info_test_by_last_id(self):
        get_users_info_test(self.driver, self.session_factory)

        self.assertRaises(UserIsInactiveException)

    def test_negative_get_users_info_test(self):
        get_users_info_test(self.driver, self.session_factory, [0])

        self.assertRaises(PageNotFound)

    def test_positive_get_users_info_test_by_last_id(self):
        users_info = pd.DataFrame({'url': [919159],
                                   'sex': [1],
                                   'favorite_tags': [{'1'}],
                                   'favorite_titles': [{'1'}],
                                   'abandoned_titles': [{'1'}]})
        add_full_users(self.session_factory, users_info)
        get_users_info_test(self.driver, self.session_factory)

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
        get_users_info_test(self.driver, self.session_factory, amount=100)

        with self.session_factory() as session:
            result = session.query(UserORM).all()
            print(result)

    def test_unknown_error(self):
        get_users_info_test(self.driver, self.session_factory, user_ids=[919215])
        self.assertRaises(AttributeError)