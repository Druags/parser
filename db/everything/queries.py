import numpy
import pandas as pd
from typing import Type
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
    user_data = get_user_data()

    # titles = get_titles_url()
    titles = expand_manga_data()
    authors = pd.read_csv(DATA_DIR + 'authors.csv', names=['name'])
    artists = pd.read_csv(DATA_DIR + 'artists.csv', names=['name'])
    publishers = pd.read_csv(DATA_DIR + 'publishers.csv', names=['name'])
    tags = pd.read_csv(DATA_DIR + 'tags.csv', names=['name'])
    models_and_data = ((UserORM, user_data), (AuthorORM, authors), (ArtistORM, artists),
                       (PublisherORM, publishers), (TagORM, tags))
    # add_m2m(session_factory, right_orm_object=TagORM, attr='tags',
    #         left_orm_object=TitleORM, data=titles.iloc[0]['tags'])
    for orm_name, data in models_and_data:
        fill_table(session_factory, orm_name=orm_name, data=data)
    add_categories(session_factory)
    fill_table(session_factory, orm_name=TitleORM, data=titles)
    add_full_title(session_factory, titles)


def add_full_title(session_factory: sessionmaker, title_data: pd.DataFrame) -> None:
    with session_factory() as session:
        for row in tqdm(title_data.itertuples()):
            title = TitleORM(url=getattr(row, 'url'),
                             release_year=getattr(row, 'release_year'),
                             chapters_uploaded=getattr(row, 'chapters_uploaded'),
                             type=get_id(session, TypeORM, getattr(row, 'type')),
                             age_rating=get_id(session, AgeRatingORM, getattr(row, 'age_rating')),
                             translation_status=get_id(session,
                                                       TranslationStatusORM,
                                                       getattr(row, 'translation_status')),
                             publication_status=get_id(session,
                                                       PublicationStatusORM,
                                                       getattr(row, 'publication_status'))
                             )
            session.add(title)

            m2m_relations = ((AuthorORM, 'authors', getattr(row, 'authors')),
                             (PublisherORM, 'publishers', getattr(row, 'publishers')),
                             (ArtistORM, 'artists', getattr(row, 'artists')),
                             (ReleaseFormatORM, 'release_formats', getattr(row, 'release_formats'))
                             )
            for orn_name, b_p_field, data in m2m_relations:
                add_m2m(session, main_object=title, right_orm_name=orn_name, b_p_field=b_p_field, data=data)

        session.flush()
        session.commit()



def add_m2m(session: Session, *,
            main_object,
            right_orm_name: Type[Base],
            b_p_field: str,
            data: set) -> None:
    for element in data:
        (getattr(main_object, b_p_field).
         append(session.query(right_orm_name).
                filter_by(id=get_id(session, right_orm_name, element)
                          ).first()
                )
         )


def get_id(session: Session, orm_name: Type[Base], name: str):
    if pd.isnull(name):
        return
    try:
        return session.query(orm_name).filter_by(name=name).first().id
    except AttributeError:
        session.add(orm_name(name=name))
        max_id = session.query(func.max(orm_name.id)).first()[0]

        return max_id


def add_categories(session_factory: sessionmaker) -> None:
    types = ['Манхва', 'Манга', 'Маньхуа', 'Комикс западный', 'OEL-манга', 'Руманга']
    publication_statuses = ['Завершён', 'Онгоинг', 'Выпуск прекращён', 'Приостановлен', 'Анонс']
    age_ratings = ['16+', '18+']
    translation_statuses = ['Завершен', 'Заброшен', 'Продолжается', 'Заморожен']
    release_formats = ['4-кома (Ёнкома)', 'В цвете', 'Веб', 'Вебтун', 'Додзинси', 'Сборник', 'Сингл']
    categories_data = [(TypeORM, types),
                       (PublicationStatusORM, publication_statuses),
                       (AgeRatingORM, age_ratings),
                       (TranslationStatusORM, translation_statuses),
                       (ReleaseFormatORM, release_formats)
                       ]
    for orm_name, categories in categories_data:
        add_category(session_factory, orm_name=orm_name, categories=categories)


def add_category(session_factory: sessionmaker, *, orm_name: Type[Base], categories: list[str]):
    with session_factory() as session:
        session.add_all([orm_name(name=name) for name in categories])
        session.flush()
        session.commit()


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


def fill_table(session_factory: sessionmaker, *, orm_name: Type[Base], data: pd.DataFrame) -> None:
    with session_factory() as session:
        session.add_all([orm_name(**{col: getattr(row, col) for col in data.columns}) for row in data.itertuples()])
        session.flush()
        session.commit()


# TODO придумать как сделать аналогичную запись в бд при парсинге без записи в цсв файл
def add_id_to_id_conn(session_factory: sessionmaker, *, orm_name: Type[Base], right_table: pd.DataFrame, right_key: str,
                      left_table: pd.DataFrame, left_key: str) -> None:
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


def select_orm(session_factory: sessionmaker, *, orm_name: Type[Base], index_name: str) -> pd.DataFrame:
    with session_factory() as session:
        result = session.query(orm_name).all()
        result = pd.DataFrame.from_records([ob.to_dict() for ob in result], index='id')
        result[index_name] = result.index
        return result
