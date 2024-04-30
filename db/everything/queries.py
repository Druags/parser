import pandas as pd
from typing import Type
from sqlalchemy.sql.expression import func
from sqlalchemy.orm.session import sessionmaker, Session
from tqdm import tqdm

from db.everything.data_work import expand_manga_data, get_user_info, get_title_info
from db.everything.models import (Base, UserORM, TitleORM, TagORM,
                                  AuthorORM, ArtistORM, PublisherORM,
                                  TranslationStatusORM, ReleaseFormatORM,
                                  PublicationStatusORM, AgeRatingORM, TypeORM, TitleRatingORM)
from db.everything import data
from db.everything.data_classes import TableContent, TableRelation
from config import DATA_DIR


def create_tables(engine) -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def fill_tables(session_factory: sessionmaker) -> None:
    titles = get_title_info(DATA_DIR + 'manga_data_expanded.csv')
    add_full_title(session_factory, titles)

    user_data = get_user_info(DATA_DIR + 'users_full_info.csv')
    add_full_user(session_factory, user_data)


def add_full_user(session_factory: sessionmaker, users_info: pd.DataFrame) -> None:
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


def add_full_title(session_factory: sessionmaker, titles_info: pd.DataFrame) -> None:
    with session_factory() as session:
        for row in tqdm(titles_info.itertuples()):
            title = TitleORM(url=getattr(row, 'url'),
                             release_year=getattr(row, 'release_year'),
                             chapters_uploaded=getattr(row, 'chapters_uploaded'),
                             type=get_id(session,
                                         orm_name=TypeORM,
                                         key_field_value=getattr(row, 'type')),
                             age_rating=get_id(session,
                                               orm_name=AgeRatingORM,
                                               key_field_value=getattr(row, 'age_rating')),
                             translation_status=get_id(session,
                                                       orm_name=TranslationStatusORM,
                                                       key_field_value=getattr(row, 'translation_status')),
                             publication_status=get_id(session,
                                                       orm_name=PublicationStatusORM,
                                                       key_field_value=getattr(row, 'publication_status'))
                             )
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
                print(main_object)
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
        getattr(main_object, b_p_field).append(join_orm_name(name=rating, qty=qty))


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
