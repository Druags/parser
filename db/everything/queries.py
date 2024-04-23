from collections import namedtuple

import numpy
import pandas as pd
from typing import Type
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import func
from sqlalchemy.orm.session import sessionmaker, Session
from tqdm import tqdm

from db.everything.data_work import get_user_data, get_pairs, get_titles_url, get_authors, expand_manga_data
from db.everything.models import (Base, UserORM, TitleORM, TagORM, FavoriteTitleORM,
                                  AbandonedTitleORM, AuthorORM, ArtistORM, PublisherORM,
                                  PublisherTitleORM, AuthorTitleORM, TranslationStatusORM, ReleaseFormatORM,
                                  PublicationStatusORM, AgeRatingORM, TypeORM)
from config import DATA_DIR


def create_tables(engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def fill_tables(session_factory: sessionmaker) -> None:
    user_data = pd.read_csv(DATA_DIR + 'users_full_info.csv')

    TableContent = namedtuple('TableContent', ['orm_name', 'table_content'])
    tables_content = (TableContent(AuthorORM, pd.read_csv(DATA_DIR + 'authors.csv', names=['name'])),
                      TableContent(ArtistORM, pd.read_csv(DATA_DIR + 'artists.csv', names=['name'])),
                      TableContent(PublisherORM, pd.read_csv(DATA_DIR + 'publishers.csv', names=['name'])),
                      TableContent(TagORM, pd.read_csv(DATA_DIR + 'tags.csv', names=['name'])))

    for orm_name, table_content in tables_content:
        df_to_orm(session_factory, orm_name=orm_name, data=table_content)
    add_categories(session_factory)

    titles = expand_manga_data()
    add_full_title(session_factory, titles)


def add_full_user(session_factory: sessionmaker, data: pd.DataFrame) -> None:
    TableRelation = namedtuple('TableRelation',
                               ['orm_name', 'bp_field_name', 'join_records'])

    with session_factory() as session:
        for row in tqdm(data.itertuples()):
            user = UserORM(url=getattr(row, 'url'),
                           sex=getattr(row, 'sex'))
            session.add(user)

            m2m_relations = (
                TableRelation(TitleORM, 'favorite_titles', getattr(row, 'favorite_titles')),
                TableRelation(TitleORM, 'abandoned_titles', getattr(row, 'abandoned_titles'))
            )

            for orn_name, b_p_field, join_records in m2m_relations:
                add_m2m(session,
                        main_object=user,
                        join_orm=orn_name,
                        b_p_field=b_p_field,
                        join_records=join_records,
                        key_field_name='url')

        session.flush()
        session.commit()


def add_full_title(session_factory: sessionmaker, data: pd.DataFrame) -> None:
    with session_factory() as session:
        for row in tqdm(data.itertuples()):
            title = TitleORM(url=getattr(row, 'url'),
                             release_year=getattr(row, 'release_year'),
                             chapters_uploaded=getattr(row, 'chapters_uploaded'),
                             type=get_id(session, TypeORM, {'name': getattr(row, 'type')}),
                             age_rating=get_id(session, AgeRatingORM, {'name': getattr(row, 'age_rating')}),
                             translation_status=get_id(session,
                                                       TranslationStatusORM,
                                                       {'name': getattr(row, 'translation_status')}),
                             publication_status=get_id(session,
                                                       PublicationStatusORM,
                                                       {'name': getattr(row, 'publication_status')})
                             )
            session.add(title)
            m2m_relations = (
                (AuthorORM, 'authors', getattr(row, 'authors')),
                (PublisherORM, 'publishers', getattr(row, 'publishers')),
                (ArtistORM, 'artists', getattr(row, 'artists')),
                (ReleaseFormatORM, 'release_formats', getattr(row, 'release_formats')),
                (TagORM, 'tags', getattr(row, 'tags'))
            )
            for orn_name, b_p_field, data in m2m_relations:
                add_m2m(session, main_object=title, join_orm=orn_name, b_p_field=b_p_field, join_records=data)

        session.flush()
        session.commit()


def add_m2m_to_existing(session: Session, *,
                        main_orm_name: Type[Base],
                        b_p_field: str,
                        add_orm_name: Type[Base],
                        data: pd.DataFrame):
    main_objects = session.query(main_orm_name).all()
    for main_object in tqdm(main_objects):
        try:
            data_part = data.loc[main_object.url][b_p_field]
        except KeyError:
            continue
        add_m2m(session,
                main_object=main_object,
                join_orm=add_orm_name,
                b_p_field=b_p_field,
                join_records=data_part)
    session.flush()
    session.commit()


def add_m2m(session: Session, *,
            main_object,
            b_p_field: str,
            join_orm: Type[Base],
            join_records: set,
            key_field_name: str = 'name') -> None:
    for record_key in join_records:
        (getattr(main_object, b_p_field).
         append(session.query(join_orm).
                filter_by(id=get_id(session,
                                    join_orm,
                                    {key_field_name: record_key})).first()
                )
         )


def get_id(session: Session, orm_name: Type[Base], n_a_v: dict):
    """
    :param session:
    :param orm_name:
    :param n_a_v: takes dict {field_name: field_value}
    :return:
    """
    if pd.isnull(n_a_v):
        return
    try:
        return session.query(orm_name).filter_by(**n_a_v).first().id
    except AttributeError:
        session.add(orm_name(**n_a_v))
        max_id = session.query(func.max(orm_name.id)).first()[0]

        return max_id


def add_categories(session_factory: sessionmaker) -> None:
    CategoryContent = namedtuple('CategoryContent', ['orm_name', 'content'])
    categories_data = [CategoryContent(TypeORM,
                                       ['Манхва', 'Манга', 'Маньхуа', 'Комикс западный', 'OEL-манга', 'Руманга']),
                       CategoryContent(PublicationStatusORM,
                                       ['Завершён', 'Онгоинг', 'Выпуск прекращён', 'Приостановлен', 'Анонс']),
                       CategoryContent(AgeRatingORM,
                                       ['16+', '18+']),
                       CategoryContent(TranslationStatusORM,
                                       ['Завершен', 'Заброшен', 'Продолжается', 'Заморожен']),
                       CategoryContent(ReleaseFormatORM,
                                       ['4-кома (Ёнкома)', 'В цвете', 'Веб', 'Вебтун', 'Додзинси', 'Сборник', 'Сингл'])
                       ]
    for orm_name, categories in categories_data:
        add_category(session_factory, orm_name=orm_name, categories=categories)


def add_category(session_factory: sessionmaker, *, orm_name: Type[Base], categories: list[str]):
    with session_factory() as session:
        session.add_all([orm_name(name=name) for name in categories])
        session.flush()
        session.commit()


# TODO можно адаптировать данные под fill_table
def add_connections(session_factory: sessionmaker) -> None:
    favorite_titles = get_pairs('E:/manga_parser/data/favorite_titles.csv')
    abandoned_titles = get_pairs('E:/manga_parser/data/abandoned_titles.csv')
    titles = select_orm(session_factory, orm_name=TitleORM, index_name='title_id')
    authors_titles = get_authors()
    n_authors = select_orm(session_factory, orm_name=AuthorORM, index_name='author_id')
    authors_titles = pd.merge(authors_titles, n_authors, left_on='author_name', right_on='name').drop_duplicates()

    connections = [(FavoriteTitleORM, favorite_titles, 'title_url', titles, 'url'),
                   (AbandonedTitleORM, abandoned_titles, 'title_url', titles, 'url'),
                   (AuthorTitleORM, authors_titles, 'title_url', titles, 'url')]

    for orm_model, right_table, right_key, left_table, left_key in connections:
        add_id_to_id_conn(session_factory,
                          orm_name=orm_model,
                          right_table=right_table,
                          right_key=right_key,
                          left_table=left_table,
                          left_key=left_key)


def df_to_orm(session_factory: sessionmaker,
              *,
              orm_name: Type[Base],
              data: pd.DataFrame) -> None:
    with session_factory() as session:
        session.add_all([orm_name(**{col: getattr(row, col) for col in data.columns}) for row in data.itertuples()])
        session.flush()
        session.commit()


# TODO придумать как сделать аналогичную запись в бд при парсинге без записи в цсв файл
def add_id_to_id_conn(session_factory: sessionmaker,
                      *,
                      orm_name: Type[Base],
                      right_table: pd.DataFrame,
                      right_key: str,
                      left_table: pd.DataFrame,
                      left_key: str) -> None:
    with session_factory() as session:
        u_id_x_t_id = left_table.merge(right=right_table,
                                       left_on=left_key,
                                       right_on=right_key
                                       )
        fields = orm_name.__annotations__.keys()
        session.add_all(
            [orm_name(**{field: getattr(row, field) for field in fields}) for row in u_id_x_t_id.itertuples()])
        session.flush()
        session.commit()


def select_orm(session_factory: sessionmaker,
               *,
               orm_name: Type[Base],
               index_name: str) -> pd.DataFrame:
    with session_factory() as session:
        result = session.query(orm_name).all()
        result = pd.DataFrame.from_records([ob.to_dict() for ob in result], index='id')
        result[index_name] = result.index
        return result


def test_query(session_factory: sessionmaker):
    data = pd.read_csv(DATA_DIR + 'users_full_info.csv')
    with session_factory() as session:
        add_m2m_to_existing(session,
                            main_orm_name=UserORM,
                            b_p_field='favorite_titles',
                            add_orm_name=TitleORM,
                            data=data[['id', 'favorite_titles']].set_index(data['id']))
        add_m2m_to_existing(session,
                            main_orm_name=UserORM,
                            b_p_field='abandoned_titles',
                            add_orm_name=TitleORM,
                            data=data[['id', 'abandoned_titles']].set_index(data['id']))
        session.flush()
        session.commit()
