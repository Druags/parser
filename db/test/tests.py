import unittest
import numpy
from parameterized import parameterized
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..everything.models import (UserORM, Base, TitleORM,
                                 AuthorORM, ArtistORM, PublisherORM,
                                 TagORM, AbandonedTitleORM, TitleTagORM)
from ..everything.queries import (fill_table, add_categories, add_id_to_id_conn,
                                  get_id, add_full_title, add_m2m_2)

from .data import *


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
            new_row = UserORM(id=1, sex=1)
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
        fill_table(self.session_factory, orm_name=orm_name, data=data)
        with self.session_factory() as session:
            result = session.query(orm_name).filter_by(id=1).first()
            self.assertEqual(result.id, 1)

    def test_positive_add_categories(self):
        add_categories(self.session_factory)

    def test_positive_add_it_to_id_connection(self):
        right_table = good_abandoned_title
        right_key = 'title_url'
        left_table = good_selected_title_data
        left_key = 'url'
        fill_table(self.session_factory, orm_name=UserORM, data=good_user_data)
        fill_table(self.session_factory, orm_name=TitleORM, data=good_title_data)
        add_id_to_id_conn(self.session_factory, orm_name=AbandonedTitleORM,
                          right_table=right_table,
                          right_key=right_key,
                          left_table=left_table,
                          left_key=left_key)
        with self.session_factory() as session:
            result = session.query(AbandonedTitleORM).first()
            self.assertEqual(result.user_id, 1)

    def test_positive_get_id(self):
        with self.session_factory() as session:
            result = get_id(session, AuthorORM, 'name')
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
        fill_table(self.session_factory, orm_name=AuthorORM, data=good_author_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=1).first()
            self.assertEqual('Test_Author', result.name)

    def test_positive_connection_of_title_and_new_author(self):
        add_full_title(self.session_factory, good_title_full_data)
        fill_table(self.session_factory, orm_name=AuthorORM, data=pd.DataFrame({'name': ['test_author']}))
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(id=2).first()
            self.assertEqual(2, result.id)

    def test_positive_add_m2m(self):
        add_full_title(self.session_factory, good_title_full_data)
        with self.session_factory() as session:
            title = session.query(TitleORM).first()
            add_m2m_2(session,
                      attr='tags',
                      left_orm_object=title,
                      right_orm_name=TagORM,
                      data=good_title_full_data.iloc[0]['tags']
                      )
            result = session.query(TitleORM).filter_by(id=1).first()
            expected = '[<TitleTagORM title_id=1, tag_id=1>, <TitleTagORM title_id=1, tag_id=2>]'
            self.assertEqual(expected, str(session.query(TitleTagORM).all()))


# TODO тест с загрузкой данных из файла
if __name__ == '__main__':
    unittest.main()
