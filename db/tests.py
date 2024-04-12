import unittest

import pandas as pd

from models import UserORM, Base, TitleORM, AuthorORM, ArtistORM, PublisherORM, TagORM, AbandonedTitleORM
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from queries import fill_table, add_categories, add_id_to_id_conn

good_user_data = pd.DataFrame({'id': [2], 'sex': [1]})
good_title_data = pd.DataFrame({'url': ['test-title-url']})
good_author_data = pd.DataFrame({'name': ['test_author_name']})
good_publisher_data = pd.DataFrame({'name': ['test_publisher_name']})
good_artist_data = pd.DataFrame({'name': ['test_artist_name']})
good_tag_data = pd.DataFrame({'name': ['test_tag_name']})
good_abandoned_title = pd.DataFrame({'user_id': [2], 'title_url': ['test-title-url']})
good_selected_title_data = pd.DataFrame({'title_id': [1], 'url': ['test-title-url']})


class TestORMModels(unittest.TestCase):
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

    def test_positive_fill_table_user(self):
        fill_table(self.session_factory, orm_name=UserORM, data=good_user_data)
        with self.session_factory() as session:
            result = session.query(UserORM).filter_by(id=2).first()
            self.assertEqual(result.id, 2)

    def test_positive_fill_table_title(self):
        fill_table(self.session_factory, orm_name=TitleORM, data=good_title_data)
        with self.session_factory() as session:
            result = session.query(TitleORM).filter_by(url='test-title-url').first()
            self.assertEqual(result.id, 1)

    def test_positive_fill_table_author(self):
        fill_table(self.session_factory, orm_name=AuthorORM, data=good_author_data)
        with self.session_factory() as session:
            result = session.query(AuthorORM).filter_by(name='test_author_name').first()
            self.assertEqual(result.id, 1)

    def test_positive_fill_table_artist(self):
        fill_table(self.session_factory, orm_name=ArtistORM, data=good_artist_data)
        with self.session_factory() as session:
            result = session.query(ArtistORM).filter_by(name='test_artist_name').first()
            self.assertEqual(result.id, 1)

    def test_positive_fill_table_publisher(self):
        fill_table(self.session_factory, orm_name=PublisherORM, data=good_publisher_data)
        with self.session_factory() as session:
            result = session.query(PublisherORM).filter_by(name='test_publisher_name').first()
            self.assertEqual(result.id, 1)

    def test_positive_fill_table_tag(self):
        fill_table(self.session_factory, orm_name=TagORM, data=good_tag_data)
        with self.session_factory() as session:
            result = session.query(TagORM).filter_by(name='test_tag_name').first()
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
            self.assertEqual(result.user_id, 2)


if __name__ == '__main__':
    unittest.main()
