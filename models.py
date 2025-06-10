import msgspec
from datetime import datetime
from typing import Optional, Dict
from dateutil import tz


def utc_to_local(dt: datetime) -> datetime:
    utc_datetime = datetime.fromtimestamp(dt, tz=tz.UTC)
    local_timezone = tz.gettz("America/Denver")
    local_datetime = utc_datetime.astimezone(local_timezone)
    return local_datetime


class Author(msgspec.Struct):
    name: str  # author.name
    flair: int = 0
    author_flair_text: Optional[str] = None

    def __post_init__(self):
        flair_text = self.author_flair_text

        if flair_text:
            try:
                flair_parts = flair_text.split(maxsplit=1)
                if flair_parts[0].isdigit():
                    self.flair = int(flair_parts[0])
                else:
                    self.flair = int(flair_parts[1].split()[1])
            except (ValueError, IndexError):
                self.flair = 0
        else:
            self.flair = 0


class Post(msgspec.Struct):
    id: str
    title: str
    created: datetime
    shortlink: str
    num_comments: int = 0
    score: int = 0
    author: Optional[Author] = None

    # def __post_init__(self):
    #     self.created = utc_to_local(self.created)
    def created_local(self):
        return utc_to_local(self.created)



class SheetPost(Post):
    sheet_id: Optional[str] = None
    sheet_url: Optional[str] = None
    sheet_dl_date: Optional[datetime] = None
    sheet_raw: Optional[str] = None

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Spreadsheet(msgspec.Struct):
    """Common column headers

    These are the most common header names in the redditor's spreadsheets.
    headers.most_common(20)
    [
        ('plastic', 175),
        ('weight', 163),
        ('condition', 142),
        ('mold', 108),
        ('color', 100),
        ('manufacturer', 94),
        ('ink', 92),
        ('price', 74),
        ('brand', 69),
        ('notes', 56),
        ('disc', 55),
        ('price (shipped)', 36),
        ('stamp', 27),
        ('sold?', 19),
        ('price shipped', 19),
        ('picture', 17),
        ('model', 17),
        ('status', 16),
        ('inked?', 15),
        ('ink?', 13)
    ]

    Args:
        BaseModel (pydantic): Pydantic BaseModel
    """

    price: float
    manufacturer: str
    mold: str
    plastic: str
    color: str
    condition: int
    ink: bool
    picture: str
    price_shipped: bool
    stamp: str
    weight: int
    status: bool
    notes: str
    weight_scaled: Optional[int] = None
    sheet_id: Optional[str] = None
    sheet_url: Optional[str] = None


# MongoDB models
class ExchangeDB(msgspec.Struct):
    etag: str
    data_json: str
    last_updated: datetime
    raw_json: Dict

    def dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

class Transaction(msgspec.Struct):
    post_id: str
    partner: str
    comment_id: Optional[str]
    timestamp: Optional[datetime] = None


class UserSwap(msgspec.Struct):
    username: str
    transactions: list[Transaction]

    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}

