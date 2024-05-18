import pandas as pd
from typing import Type, Union

from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import func
from sqlalchemy.orm.session import sessionmaker, Session
from tqdm import tqdm

from db.everything.data_work import get_user_info, get_title_info
from db.everything.models import (Base, UserORM, TitleORM, TagORM,
                                  AuthorORM, ArtistORM, PublisherORM,
                                  TranslationStatusORM, ReleaseFormatORM,
                                  PublicationStatusORM, AgeRatingORM, TypeORM, TitleRatingORM)
from db.everything.data_classes import TableRelation
from config import DATA_DIR


def create_tables(engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def fill_tables(session_factory: sessionmaker) -> None:
    titles = get_title_info(DATA_DIR + 'manga_data_expanded.csv')
    add_full_titles(session_factory, titles)

    user_data = get_user_info(DATA_DIR + 'users_full_info.csv')
    add_full_users(session_factory, user_data)


def add_full_users(session_factory: sessionmaker,
                   users_info: pd.DataFrame) -> None:
    with session_factory() as session:
        for row in tqdm(users_info.itertuples()):
            user = UserORM(url=getattr(row, 'url'),
                           sex=getattr(row, 'sex'))
            session.add(user)
            m2m_relations = (
                TableRelation(TitleORM, 'favorite_titles', getattr(row, 'favorite_titles')),
                TableRelation(TitleORM, 'abandoned_titles', getattr(row, 'abandoned_titles'))
            )

            for element in m2m_relations:
                add_m2m(session,
                        main_object=user,
                        join_orm_name=element.orm_name,
                        b_p_field=element.bp_field_name,
                        join_records=element.join_records,
                        key_field_name=element.key_field_name)

            session.flush()
        session.commit()


def add_full_titles(session_factory: sessionmaker, titles_info: pd.DataFrame) -> None:
    with session_factory() as session:
        for row in tqdm(titles_info.itertuples()):
            title = TitleORM(url=row.url)

            title.release_year = getattr(row, 'release_year')
            title.chapters_uploaded = getattr(row, 'chapters_uploaded')

            o2m_relations = [(TypeORM, 'type'),
                             (AgeRatingORM, 'age_rating'),
                             (TranslationStatusORM, 'translation_status'),
                             (PublicationStatusORM, 'publication_status')]
            for relation in o2m_relations:
                setattr(title,
                        relation[1],
                        get_id(session, orm_name=relation[0], key_field_value=getattr(row, relation[1])))

            session.add(title)
            m2m_relations = (
                TableRelation(AuthorORM, 'authors', getattr(row, 'authors')),
                TableRelation(PublisherORM, 'publishers', getattr(row, 'publishers')),
                TableRelation(ArtistORM, 'artists', getattr(row, 'artists')),
                TableRelation(ReleaseFormatORM, 'release_formats', getattr(row, 'release_formats')),
                TableRelation(TagORM, 'tags', getattr(row, 'tags'))
            )

            for element in m2m_relations:
                add_m2m(session,
                        main_object=title,
                        join_orm_name=element.orm_name,
                        b_p_field=element.bp_field_name,
                        join_records=element.join_records)

            add_o2m(main_object=title,
                    b_p_field='ratings',
                    join_data=getattr(row, 'ratings'),
                    join_orm_name=TitleRatingORM)

        session.flush()
        session.commit()


def add_o2m_to_existing(session_factory: sessionmaker,
                        main_orm_name: Type[Base],
                        b_p_field: str,
                        join_orm_name: Type[Base],
                        join_data: pd.DataFrame):
    with session_factory() as session:
        main_objects = session.query(main_orm_name).all()

        for main_object in tqdm(main_objects):
            try:
                data_part = join_data.loc[main_object.url][b_p_field]
            except KeyError:
                continue
            add_o2m(main_object=main_object,
                    b_p_field=b_p_field,
                    join_orm_name=join_orm_name,
                    join_data=data_part)
        session.flush()
        session.commit()


def add_o2m(main_object,
            b_p_field: str,
            join_orm_name: Type[Base],
            join_data: dict):
    for rating, qty in join_data.items():
        (getattr(main_object, b_p_field).
         append(join_orm_name(name=rating, qty=qty)))


# TODO создаёт дубликаты, поскольку не проверяет,
#  что объект уже существует, для оценок это важно

def add_m2m_to_existing(session: Session, *,
                        main_orm_name: Type[Base],
                        b_p_field: str,
                        join_orm_name: Type[Base],
                        join_data: pd.DataFrame):
    main_objects = session.query(main_orm_name).all()
    for main_object in tqdm(main_objects):
        try:
            data_part = join_data.loc[main_object.url][b_p_field]
        except KeyError:
            continue
        add_m2m(session,
                main_object=main_object,
                join_orm_name=join_orm_name,
                b_p_field=b_p_field,
                join_records=data_part)
    session.flush()
    session.commit()


def add_m2m(session: Session, *,
            main_object,
            b_p_field: str,
            join_orm_name: Type[Base],
            join_records: set,
            key_field_name: str = 'name') -> None:
    for record_key in join_records:
        (getattr(main_object, b_p_field).
         append(session.query(join_orm_name).
                filter_by(id=get_id(session,
                                    orm_name=join_orm_name,
                                    key_field_name=key_field_name,
                                    key_field_value=record_key)).first()
                )
         )


def get_id(session: Session, *, orm_name: Type[Base], key_field_value: str, key_field_name: str = 'name'):
    if pd.isnull(key_field_value):
        return
    try:
        return session.query(orm_name).filter_by(**{key_field_name: key_field_value}).first().id
    except AttributeError:
        session.add(orm_name(**{key_field_name: key_field_value}))
        max_id = session.query(func.max(orm_name.id)).first()[0]

        return max_id


def df_to_orm(session_factory: sessionmaker,
              *,
              orm_name: Type[Base],
              converted_data: pd.DataFrame) -> None:
    with session_factory() as session:
        session.add_all([orm_name(**{col: getattr(row, col) for col in converted_data.columns})
                         for row in converted_data.itertuples()])
        session.flush()
        session.commit()


def get_max_user_url(session_factory: sessionmaker) -> int:
    with session_factory() as session:
        try:
            max_url = session.query(func.max(UserORM.url)).first()[0]
            if max_url is None:
                max_url = 3
        except Exception as e:
            print(e)
            max_url = 3

    return max_url


def get_object_with_empty_field(session_factory: sessionmaker):
    with session_factory() as session:
        url_tuples = (session.query(TitleORM.url).
                      filter(TitleORM.type == None).
                      all())
        result = [url[0] for url in url_tuples]
        return result


def update_title(session_factory: sessionmaker,
                 title_info: dict):
    with session_factory() as session:
        title = session.query(TitleORM).where(TitleORM.url == title_info.get('url')).one()

        title.release_year = title_info.get('release_year')
        title.chapters_uploaded = title_info.get('chapters_uploaded')

        o2m_relations = [
            (TypeORM, 'type'),
            (AgeRatingORM, 'age_rating'),
            (TranslationStatusORM, 'translation_status'),
            (PublicationStatusORM, 'publication_status')]
        for relation in o2m_relations:
            setattr(title,
                    relation[1],
                    get_id(session, orm_name=relation[0], key_field_value=title_info.get(relation[1])))

        session.add(title)
        m2m_relations = (
            TableRelation(AuthorORM, 'authors', title_info.get('authors')),
            TableRelation(PublisherORM, 'publishers', title_info.get('publishers')),
            TableRelation(ArtistORM, 'artists', title_info.get('artists')),
            TableRelation(ReleaseFormatORM, 'release_formats', title_info.get('release_formats')),
            TableRelation(TagORM, 'tags', title_info.get('tags'))
        )

        for element in m2m_relations:
            add_m2m(session,
                    main_object=title,
                    join_orm_name=element.orm_name,
                    b_p_field=element.bp_field_name,
                    join_records=element.join_records)

        add_o2m(main_object=title,
                b_p_field='ratings',
                join_data=title_info.get('ratings'),
                join_orm_name=TitleRatingORM)
        session.commit()


def add_full_user(session_factory: sessionmaker,
                  user_info: pd.DataFrame) -> None:
    with session_factory() as session:

        user = UserORM(url=getattr(user_info, 'url'),
                       sex=getattr(user_info, 'sex'))
        session.add(user)
        m2m_relations = (
            TableRelation(TitleORM, 'favorite_titles', getattr(user_info, 'favorite_titles')),
            TableRelation(TitleORM, 'abandoned_titles', getattr(user_info, 'abandoned_titles'))
        )

        for element in m2m_relations:
            add_m2m(session,
                    main_object=user,
                    join_orm_name=element.orm_name,
                    b_p_field=element.bp_field_name,
                    join_records=element.join_records,
                    key_field_name=element.key_field_name)

        session.flush()
        session.commit()
