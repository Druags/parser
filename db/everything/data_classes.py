from dataclasses import dataclass

import pandas as pd
from db.everything.models import Base


@dataclass
class TableContent:
    orm_name: type[Base]
    content: pd.DataFrame


@dataclass
class TableRelation:
    orm_name: type[Base]
    bp_field_name: str
    join_records: set
    key_field_name: str = 'url'
