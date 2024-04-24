import unittest

import numpy
from parameterized import parameterized
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.everything.models import (UserORM, Base, TitleORM,
                                  AuthorORM, ArtistORM, PublisherORM,
                                  TagORM, AbandonedTitleORM, TitleTagORM, PublisherTitleORM, FavoriteTitleORM)
from db.everything.queries import (df_to_orm, add_categories,
                                   get_id, add_full_title, add_m2m, add_m2m_to_existing, add_full_user)
from db.test.data import *


class TestQueries(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(bind=self.engine)
        self.session_factory = sessionmaker(bind=self.engine)

    def tearDown(self):
        with self.session_factory() as session:
            Base.metadata.drop_all(bind=self.engine)
            session.close()

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
            result = get_id(session, AuthorORM, {'name': 'name'})
            self.assertEqual(1, result)

    def test_negative_get_id(self):
        with self.session_factory() as session:
            result = get_id(session, AuthorORM, numpy.nan)
            self.assertEqual(None, result)

    def test_positive_add_full_title(self):
        add_full_title(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(TitleORM).filter_by(id=1).first()
            self.assertEqual(1, result.id)

    def test_positive_connection_of_title_and_author(self):
        add_full_title(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=1).first()

            self.assertEqual('Test_Author', result.name)

    def test_positive_connection_of_title_and_existing_author(self):
        add_full_title(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=1).first()
            self.assertEqual('Test_Author', result.name)

    def test_positive_connection_of_title_and_new_author(self):
        add_full_title(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=2).first()
            self.assertEqual(2, result.id)

    def test_positive_add_m2m(self):
        add_full_title(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            expected = '[<TitleTagORM title_id=1, tag_id=1>, <TitleTagORM title_id=1, tag_id=2>]'
            self.assertEqual(expected, str(session.query(TitleTagORM).filter_by(title_id=1).all()))

    def test_positive_update_object(self):
        add_full_title(self.session_factory, good_title_full_data)
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
        add_full_user(self.session_factory, good_user_full_data)
        with self.session_factory() as session:
            result = session.query(TitleORM).all()

            self.assertEqual(3, len(result))


# TODO тест с загрузкой данных из файла
if __name__ == '__main__':
    unittest.main()
