from sqlalchemy import String, ForeignKey, SmallInteger, Integer
from sqlalchemy.orm import mapped_column, Mapped, relationship
from sqlalchemy.orm import DeclarativeBase
from typing import Annotated

int_pk = Annotated[int, mapped_column(primary_key=True)]


class Base(DeclarativeBase):
    def __repr__(self):
        self.repr_cols_num = 3
        self.repr_cols = tuple()
        cols = []
        for idx, col in enumerate(self.__table__.columns.keys()):
            if col in self.repr_cols or idx < self.repr_cols_num:
                cols.append(f"{col}={getattr(self, col)}")

        return f"<{self.__class__.__name__} {', '.join(cols)}>"


class UserORM(Base):
    __tablename__ = 'user'

    id: Mapped[int_pk]
    url: Mapped[int] = mapped_column(Integer, unique=True)
    sex: Mapped[int] = mapped_column(SmallInteger)

    favorite_titles: Mapped[list['TitleORM']] = relationship(
        back_populates='on_the_favorites',
        secondary='favorite_title'
    )
    abandoned_titles: Mapped[list['TitleORM']] = relationship(
        back_populates='on_the_abandoned',
        secondary='abandoned_title'
    )

    favorite_tags: Mapped[list['TagORM']] = relationship(
        back_populates='users',
        secondary='user_favorite_tag'
    )

    def to_dict(self):
        return {'id': self.id, 'sex': self.sex}


class TitleORM(Base):
    __tablename__ = 'title'

    id: Mapped[int_pk]
    url: Mapped[str] = mapped_column(String(300), unique=True)

    release_year: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    chapters_uploaded: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    type: Mapped[int] = mapped_column(ForeignKey('type.id'), nullable=True)
    age_rating: Mapped[int] = mapped_column(ForeignKey('age_rating.id'), nullable=True)
    translation_status: Mapped[int] = mapped_column(ForeignKey('translation_status.id'), nullable=True)
    publication_status: Mapped[int] = mapped_column(ForeignKey('publication_status.id'), nullable=True)

    ratings: Mapped[list['TitleRatingORM']] = relationship(back_populates='title')

    release_formats: Mapped[list['ReleaseFormatORM']] = relationship(
        back_populates='titles',
        secondary='title_release_format'
    )

    publishers: Mapped[list['PublisherORM']] = relationship(
        back_populates='titles',
        secondary='publisher_title'
    )

    authors: Mapped[list['AuthorORM']] = relationship(
        back_populates='titles',
        secondary='author_title'
    )
    artists: Mapped[list['ArtistORM']] = relationship(
        back_populates='titles',
        secondary='artist_title'
    )
    on_the_favorites: Mapped[list['UserORM']] = relationship(
        back_populates='favorite_titles',
        secondary='favorite_title'
    )
    on_the_abandoned: Mapped[list['UserORM']] = relationship(
        back_populates='abandoned_titles',
        secondary='abandoned_title'
    )
    tags: Mapped[list['TagORM']] = relationship(
        back_populates='titles',
        secondary='title_tag'
    )

    def to_dict(self):
        return {'id': self.id, 'url': self.url}


class TitleRatingORM(Base):
    __tablename__ = 'title_rating'

    id: Mapped[int_pk]
    name: Mapped[SmallInteger] = mapped_column(SmallInteger)
    qty: Mapped[int] = mapped_column(Integer)
    title_id: Mapped[int] = mapped_column(ForeignKey('title.id'))

    title: Mapped['TitleORM'] = relationship(back_populates='ratings')


class ArtistORM(Base):
    __tablename__ = 'artist'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(70), unique=True)

    titles: Mapped[list['TitleORM']] = relationship(
        back_populates='artists',
        secondary='artist_title'
    )


class PublisherORM(Base):
    __tablename__ = 'publisher'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(60), unique=True)
    titles: Mapped[list['TitleORM']] = relationship(
        back_populates='publishers',
        secondary='publisher_title'
    )


class ReleaseFormatORM(Base):
    __tablename__ = 'release_format'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(15))
    titles: Mapped[list['TitleORM']] = relationship(
        back_populates='release_formats',
        secondary='title_release_format'
    )


class TitleReleaseFormatORM(Base):
    __tablename__ = 'title_release_format'

    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True)
    release_format_id: Mapped[int] = mapped_column(
        ForeignKey('release_format.id', ondelete='CASCADE'),
        primary_key=True)


class TranslationStatusORM(Base):
    __tablename__ = 'translation_status'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(12))
    titles: Mapped[list['TitleORM']] = relationship()


class PublicationStatusORM(Base):
    __tablename__ = 'publication_status'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(16))
    titles: Mapped[list['TitleORM']] = relationship()


class AgeRatingORM(Base):
    __tablename__ = 'age_rating'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(3))
    titles: Mapped[list['TitleORM']] = relationship()


class TypeORM(Base):
    __tablename__ = 'type'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(30))
    titles: Mapped[list['TitleORM']] = relationship()


class AuthorORM(Base):
    __tablename__ = 'author'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(60), unique=True)

    titles: Mapped[list['TitleORM']] = relationship(
        back_populates='authors',
        secondary='author_title'
    )

    def to_dict(self):
        return {'id': self.id, 'name': self.name}


class TagORM(Base):
    __tablename__ = 'tag'

    id: Mapped[int_pk]
    name: Mapped[str] = mapped_column(String(28))

    titles: Mapped[list['TitleORM']] = relationship(
        back_populates='tags',
        secondary='title_tag'
    )

    users: Mapped[list['UserORM']] = relationship(
        back_populates='favorite_tags',
        secondary='user_favorite_tag'
    )

    def to_dict(self):
        return {'id': self.id, 'name': self.name}


class ArtistTitleORM(Base):
    __tablename__ = 'artist_title'

    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True
    )

    artist_id: Mapped[int] = mapped_column(
        ForeignKey('artist.id', ondelete='CASCADE'),
        primary_key=True
    )


class PublisherTitleORM(Base):
    __tablename__ = 'publisher_title'

    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True
    )
    publisher_id: Mapped[int] = mapped_column(
        ForeignKey('publisher.id', ondelete='CASCADE'),
        primary_key=True
    )


class AuthorTitleORM(Base):
    __tablename__ = 'author_title'

    author_id: Mapped[int] = mapped_column(
        ForeignKey('author.id', ondelete='CASCADE'),
        primary_key=True
    )
    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True
    )


class UserFavoriteTagORM(Base):
    __tablename__ = 'user_favorite_tag'

    user_id: Mapped[int] = mapped_column(
        ForeignKey('user.id', ondelete='CASCADE'),
        primary_key=True
    )

    tag_id: Mapped[int] = mapped_column(
        ForeignKey('tag.id', ondelete='CASCADE'),
        primary_key=True
    )


class AbandonedTitleORM(Base):
    __tablename__ = 'abandoned_title'

    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey('user.id', ondelete='CASCADE'),
        primary_key=True
    )


class FavoriteTitleORM(Base):
    __tablename__ = 'favorite_title'

    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey('user.id', ondelete='CASCADE'),
        primary_key=True
    )


class TitleTagORM(Base):
    __tablename__ = 'title_tag'

    title_id: Mapped[int] = mapped_column(
        ForeignKey('title.id', ondelete='CASCADE'),
        primary_key=True
    )
    tag_id: Mapped[int] = mapped_column(
        ForeignKey('tag.id', ondelete='CASCADE'),
        primary_key=True
    )
