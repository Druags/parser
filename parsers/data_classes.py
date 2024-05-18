from dataclasses import dataclass, fields


@dataclass
class UserInfo:
    url: int
    sex: int
    f_tags: dict
    favorite_titles: set
    abandoned_titles: set

    def __iter__(self):
        for field in fields(self):
            yield getattr(self, field.name)


class UserIsInactiveException(Exception):
    pass


class PageNotFound(Exception):
    pass
