import time
import unittest

import numpy
from parameterized import parameterized
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.everything.data_work import get_user_info, get_title_info
from db.everything.models import (UserORM, Base, TitleORM,
                                  AuthorORM, ArtistORM, PublisherORM,
                                  TagORM, TitleTagORM, FavoriteTitleORM, TitleRatingORM)
from db.everything.queries import (df_to_orm, get_id, add_full_titles, add_m2m, add_m2m_to_existing, add_full_users,
                                   add_o2m, add_o2m_to_existing, get_max_user_url)
from db.test.data import *
from config import DATA_DIR


class TestQueries(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(bind=self.engine)
        self.session_factory = sessionmaker(bind=self.engine)
        self.startTime = time.time()

    def tearDown(self):
        with self.session_factory() as session:
            Base.metadata.drop_all(bind=self.engine)
            session.close()
        t = time.time() - self.startTime
        print('%s: %.3f' % (self.id(), t))

    def test_add_row_to_table(self):
        with self.session_factory() as session:
            new_row = UserORM(id=1, url=1, sex=1)
            session.add(new_row)
            session.commit()

            result = session.query(UserORM).filter_by(id=1).first()
        self.assertEqual(result.id, 1)

    @parameterized.expand([[UserORM, good_user_data],
                           [TitleORM, good_title_data],
                           [TagORM, good_tag_data],
                           [ArtistORM, good_artist_data],
                           [AuthorORM, good_author_data],
                           [PublisherORM, good_publisher_data]])
    def test_positive_fill_table(self, orm_name, data):
        df_to_orm(self.session_factory, orm_name=orm_name, converted_data=data)
        with self.session_factory() as session:
            result = session.query(orm_name).filter_by(id=1).first()
            self.assertEqual(result.id, 1)

    def test_positive_get_id(self):
        with self.session_factory() as session:
            result = get_id(session, orm_name=AuthorORM, key_field_value='name')
            self.assertEqual(1, result)

    def test_negative_get_id(self):
        with self.session_factory() as session:
            result = get_id(session, orm_name=AuthorORM, key_field_value=numpy.nan)
            self.assertEqual(None, result)

    def test_positive_add_full_title(self):
        add_full_titles(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(TitleORM).filter_by(id=1).first()
            self.assertEqual(1, result.id)

    def test_positive_connection_of_title_and_author(self):
        add_full_titles(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=1).first()

            self.assertEqual('Test_Author', result.name)

    def test_positive_connection_of_title_and_existing_author(self):
        add_full_titles(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=1).first()
            self.assertEqual('Test_Author', result.name)

    def test_positive_connection_of_title_and_new_author(self):
        add_full_titles(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=2).first()
            self.assertEqual(2, result.id)

    def test_positive_add_m2m(self):
        add_full_titles(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            expected = '[<TitleTagORM title_id=1, tag_id=1>, <TitleTagORM title_id=1, tag_id=2>]'
            self.assertEqual(expected, str(session.query(TitleTagORM).filter_by(title_id=1).all()))

    def test_positive_update_object(self):
        add_full_titles(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            add_m2m_to_existing(session,
                                main_orm_name=TitleORM,
                                b_p_field='tags',
                                join_orm_name=TagORM,
                                join_data=good_title_tag_data.set_index(good_title_tag_data['url']))
            expected = '[<TitleTagORM title_id=1, tag_id=1>, <TitleTagORM title_id=1, tag_id=2>, <TitleTagORM title_id=1, tag_id=3>]'
            self.assertEqual(expected, str(session.query(TitleTagORM).filter_by(title_id=1).all()))

    def test_positive_add_favorite_titles(self):
        df_to_orm(self.session_factory, orm_name=UserORM, converted_data=good_user_full_data[['url', 'sex']])
        with self.session_factory() as session:
            user = session.query(UserORM).first()
            add_m2m(session,
                    main_object=user,
                    join_orm_name=TitleORM,
                    b_p_field='favorite_titles',
                    join_records=good_user_full_data.iloc[0]['favorite_titles'],
                    key_field_name='url')
            session.flush()
            session.commit()
        expected = '[<FavoriteTitleORM title_id=1, user_id=1>, <FavoriteTitleORM title_id=2, user_id=1>]'
        real = str(session.query(FavoriteTitleORM).all())
        self.assertEqual(expected, real)

    def test_positive_add_full_user(self):
        add_full_users(self.session_factory, good_user_full_data)
        with self.session_factory() as session:
            result = session.query(TitleORM).all()
            self.assertEqual(3, len(result))

    def test_positive_add_full_user_file(self):
        user_full_data = get_user_info(DATA_DIR + 'users_full_info.csv').iloc[0:2]
        add_full_users(self.session_factory, user_full_data)

        with self.session_factory() as session:
            result = session.query(FavoriteTitleORM).all()
            self.assertEqual(42, len(result))

    def test_positive_add_full_title_file(self):
        title_full_data = get_title_info(DATA_DIR + 'manga_data_expanded.csv').iloc[0:2]
        add_full_titles(self.session_factory, title_full_data)

        with self.session_factory() as session:
            result = session.query(TitleORM).first()
            self.assertEqual('mayabi', result.url)

    def test_positive_add_o2m(self):
        good_data = good_title_full_data.iloc[1:3]
        ratings = {10: 100}
        add_full_titles(self.session_factory, good_data)

        with self.session_factory() as session:
            title = session.query(TitleORM).first()
            add_o2m(main_object=title,
                    b_p_field='ratings',
                    join_data=ratings,
                    join_orm_name=TitleRatingORM)
            session.flush()
            session.commit()
            result = session.query(TitleRatingORM).first()

            self.assertEqual(result.qty, 100)

    def test_positive_add_o2m_to_existing(self):
        good_data = good_title_full_data.iloc[0:2]
        add_full_titles(self.session_factory, good_data)
        add_o2m_to_existing(self.session_factory,
                            main_orm_name=TitleORM,
                            join_orm_name=TitleRatingORM,
                            b_p_field='ratings',
                            join_data=good_data.set_index(good_data['url']))
        with self.session_factory() as session:
            result = session.query(TitleRatingORM).first().name
            self.assertEqual(10, result)

    def test_positive_add_o2m_to_existing_file(self):
        title_full_data = get_title_info(DATA_DIR + 'manga_data_expanded.csv').iloc[0:2]
        add_full_titles(self.session_factory, title_full_data)
        add_o2m_to_existing(self.session_factory,
                            main_orm_name=TitleORM,
                            join_orm_name=TitleRatingORM,
                            b_p_field='ratings',
                            join_data=title_full_data.set_index(title_full_data['url']))
        with self.session_factory() as session:
            result = session.query(TitleRatingORM).all()
            self.assertEqual(22, len(result))

    def test_positive_get_user_max_url(self):
        add_full_users(self.session_factory, good_user_full_data)
        result = get_max_user_url(self.session_factory)
        self.assertEqual(1, result)


if __name__ == '__main__':
    unittest.main()
