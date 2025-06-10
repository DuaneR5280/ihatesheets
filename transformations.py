from dataclasses import dataclass, asdict
import pandas as pd
from models import Spreadsheet, SheetPost
from collections import Counter
from typing import Optional
import re
from rich.console import Console
from cache import RedisCache


cache = RedisCache()
console = Console()

#### Working on identifying headers ####
"""
This will work better if we normalize the header names using mappings.
Use first before trying these methods.
"""
@dataclass
class Headers:
    df: pd.DataFrame
    sheet_post: SheetPost  # Model Struct
    found: bool
    col_trans: Optional[bool] = False
    match_count: Optional[int] = 0
    loc: Optional[int ] = None
    col_values: Optional[list] = None

    def __repr__(self):
        temp_sheet_post = self.sheet_post.to_dict()
        del temp_sheet_post['sheet_raw']
        output_sheet_post = SheetPost(**temp_sheet_post)
        output_data = f"""\tHeaders(
            sheet_post =
              {output_sheet_post},
            found = {self.found},
            match_count = {self.match_count},
            loc = {self.loc},
            col_values = {self.col_values},
            col_trans = {self.col_trans}
            )"""
        return output_data
    
    def asdict(self):
        return asdict(self)

mapping_keys = list(Spreadsheet.__struct_fields__)
mappings_list = [
    ('price', 'bin', 'buy it now', 'amount', 'cost'),
    ('manuf','company', 'brand', 'mfg'),
    ('mold', 'mold name', 'model', 'disc', 'name'),
    ('plastic',),
    ('color', 'colour'),
    ('condition', 'cond.', 'rating'),
    ('ink', 'marked'),
    ('image', 'photo', 'pic'),
    ('shipp',),
    ('stamp',),
    ('weight', 'grams'),
    ('status',),
    ('note', 'comment', 'details'),
    ('scale',),
]
mappings = dict(zip(mapping_keys, mappings_list))


def df_lower_strip(dataframe):
    return dataframe.columns.str.lower().str.strip().tolist()


def unique_headers(dataframes: list) -> list:
    """Unique Headers across DataFrames

    Args:
        dataframes (list): downloaded sheet data

    Returns:
        list: Unique column headers, sorted
    """
    headers = set()
    for df in dataframes:
        head_list = df_lower_strip(df)
        headers.update(head_list)
    return sorted(list(set(headers)))


def header_counter(dataframes: list) -> Counter:
    headers = []
    for df in dataframes:
        col = df_lower_strip(df)
        headers.extend(col)
    headers = list(filter(lambda x: 'unnamed:' not in x, headers))
    headers = list(filter(lambda x: re.match(r"\d+", x) is None, headers))
    return Counter(headers)


def header_match_count(row: list) -> int:
    mapping_keys_counter = Counter(mapping_keys)
    matches = {key: mapping_keys_counter[key] for key in row}
    return sum(matches.values())


def header_find(dataframe):
    df_cols = df_lower_strip(dataframe)
    count = header_match_count(df_cols)
    if count >= 2:
        return {"loc": 'column', "col_values": df_cols, "match_count": count, "found": True}
    else:
        search_rows_result = search_rows_for_header(dataframe)
        if isinstance(search_rows_result, int):
            df_cols = dataframe.iloc[search_rows_result]
            df_cols =list(filter(lambda x: x is not None, df_cols))
            df_cols = list(map(lambda x: x.lower().strip(), df_cols))
            count = header_match_count(df_cols)
            if count > 2:
                return {"loc": search_rows_result, "col_values": df_cols, "match_count": count, "found": True}
            else:
                return None
        else:
            return None


def search_rows_for_header(dataframe) -> Optional[int]:
    lower_strip = lambda x: x.lower().strip() if isinstance(x, str) else x
    dataframe.fillna('', inplace=True)
    for i, row in dataframe.iterrows():
        values = list(row.values)
        if not all(v is None for v in values) or values:
            values = list(filter(lambda x: x is not None, values))
            values = list(map(lower_strip, values))
            if any(header in values for header in mapping_keys):
                return int(i)
    return None


def map_headers(headers: list, mappings: dict) -> list:
    new_headers = []
    for header in headers:
        mapped = False
        for key, possible_values in mappings.items():
            if header.lower() in possible_values:
                new_headers.append(key)
                mapped = True
                break
            else:
                for possible_value in possible_values:
                    if possible_value.lower() in header.lower():
                        new_headers.append(key)
                        mapped = True
                        break
        if not mapped:
            new_headers.append(header)  # Keep the original header if no match found
    return new_headers


def replace_headers(headers: list):
    for h in headers:
        if h.found:
            new_headers = map_headers(h.col_values, mappings)
            console.log(f"OLD: ({len(h.col_values)}) - {h.col_values} \nNEW: ({len(new_headers)}) - {new_headers}")
            if len(new_headers) == len(h.col_values):
                h.df.columns = new_headers
                h.col_trans = True

def header_format(data: Headers) -> pd.DataFrame:
    if data.found and data.loc != 'column':
        data.df.columns = data.col_values
        data.df = data.df[data.loc +1:]
        data.df.columns = [h for h in data.df.columns]
        data.df = data.df.reset_index(drop=True)
    elif data.found and data.loc == 'column':
        data.df.columns = [h for h in data.col_values]
    return data


def header_parse() -> list:
    console.log("Parsing Headers...")
    sheet_posts = cache.get_all_hash()
    sheet_dfs = [pd.DataFrame(sheet.sheet_raw) for sheet in sheet_posts]
    header_find_results = [header_find(df) for df in sheet_dfs]
    console.log(f"Found {sum(x is not None for x in header_find_results)} headers")
    console.log(f"Missing {sum(x is None for x in header_find_results)} headers")
    console.log("Sorting Headers...")
    results = []
    for (idx, h) in enumerate(header_find_results):
        if h is not None:
            headers = Headers(
                **h,
                df = sheet_dfs[idx],
                sheet_post = sheet_posts[idx],
            )
            results.append(headers)
        else:
            headers = Headers(
                sheet_post = sheet_posts[idx],
                df = sheet_dfs[idx],
                match_count = 0,
                found = False,
            )
            results.append(headers)
    console.log("Done parsing headers")
    # results = sorted(results, key=lambda x: x.sheet_post.created.date(), reverse=True)
    formatted_headers = [header_format(x) for x in results]
    return formatted_headers

if __name__ == "__main__":
    # TESTING
    headers = header_parse()
