"""
Search the /r/<subreddit-name> for post titles with "spreadsheet"
Search found post for Google Sheets share links
Download sheet as CSV using Pandas, fallback to gspread if Pandas fails
Save CSV as file named as the document ID
"""
import pandas as pd
import time
from datetime import datetime
import re
from rich.console import Console
from rich.logging import RichHandler
from rich import print
from pathlib import Path
import logging
import gspread
from gspread import utils as gutils
from cache import RedisCache
from reddit import reddit, SUBREDDIT
from collections import Counter
from models import SheetPost, Author, Spreadsheet, utc_to_local
from typing import Optional
from dataclasses import dataclass, asdict

# Init logging
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler("./logs/sheets.log")
file_handler.setFormatter(file_formatter)

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True), file_handler],
)

logger = logging.getLogger("rich")
console = Console()

# Setup
gc = gspread.service_account(filename="creds.json")
cache = RedisCache()
BASE_URL = "https://docs.google.com/"
EXPORT_URL = "/export?format=csv&gid=0"


# Set subreddit
sr = reddit.subreddit(SUBREDDIT)


def parse_submissions(submissions: list):
    sheet_posts = []
    for post in submissions:
        console.log("Processing post...")
        local_utc = utc_to_local(post.created_utc).strftime('%Y-%m-%d %H:%M')
        logger.info(f"{local_utc} - {post.id} - {post.title[:50]} - {post.shortlink}")
        cached = cache.exists(f"post:{post.id}")

        if cached:
            logger.info(f"Cache hit post: {post.id}, skipping!!!")

        else:
            logger.info(f"Cache miss post: {post.id}")
            sheet_url = find_sheet_url(post)

            if sheet_url:
                sheet_id = gen_doc_id(sheet_url)
                logger.info(f"Sheet ID: {sheet_id}")
                submission = SheetPost(
                    id=post.id,
                    title=post.title,
                    created=datetime.fromtimestamp(post.created_utc),
                    shortlink=post.shortlink,
                    num_comments=post.num_comments,
                    score=post.score,
                    author=Author(
                        name=post.author.name,
                        author_flair_text=post.author_flair_text,
                    ),
                    sheet_id=sheet_id,
                    sheet_url=sheet_url,
                )

                logger.info(f"SheetPost created: {submission}")
                cache.set(f"post:{submission.id}", submission)
                sheet_posts.append(submission)
            else:
                submission = SheetPost(
                    id=post.id,
                    title=post.title,
                    created=datetime.fromtimestamp(post.created_utc),
                    shortlink=post.shortlink,
                    num_comments=post.num_comments,
                    score=post.score,
                    author=Author(
                        name=post.author.name,
                        author_flair_text=post.author_flair_text,
                    ),
                    sheet_id=None,
                    sheet_url=None,
                )
                cache.set(f"post:{post.id}", submission)

    return sheet_posts


def gen_doc_id(sheet_url) -> str:
    """Generate Document ID

    Extract the document id from the shared url.

    Examples:
        - "https://docs.google.com/spreadsheets/d/MGAxiQwHHr2tSZk2DjU7hzOSc3zCWImyUAvehuwczduk/edit?usp=sharing"
        - "https://docs.google.com/spreadsheets/d/ILGQFyXM3JR9PF6WP27C5k17MRUpabq2lDeMPfOUMpsJ/edit"
        - "https://docs.google.com/spreadsheets/d/0yZXBYG0UrV0TMcQZXEOPIyBMFl56rwLAiHtvjhZq1w5"
        - "https://docs.google.com/spreadsheets/u/0/d/G9gxw9liqtpGYeea8Jdm7ToHSG731jDWZ0Fx6DUQL1gU/htmlview"
        - "https://docs.google.com/spreadsheets/d/e/WVZM4GpYtAeRocXsGc8G9LdciQs3KAqXxgjiQkUtycbo/pubhtml"
        - "https://docs.google.com/document/d/mePFEWTzMxpLsCLgxmkWbvXe111iU72GRfjFsSqjKDQq/edit"  # fools be pasting sheets into docs
    Args:
        self.share_url (str): Google Sheets shared URL

    Returns:
        str: slice of the URL that contains the document ID
    """
    logging.info(f"Extracting sheet ID from: {sheet_url}")
    patterns = [
        r"/spreadsheets/d/e/([a-zA-Z0-9-_]+)",
        r"/spreadsheets/u/0/d/([a-zA-Z0-9-_]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, sheet_url)
        if match:
            return match.group(1)

    try:
        sheet_id = gutils.extract_id_from_url(sheet_url)
        return sheet_id
    except gspread.exceptions.NoValidUrlKeyFound:
        logger.error(f"gutils No valid URL key found: {sheet_url}")
        return None


def clean_url(comment: str) -> str:
    """Find Google Sheets shared URL in comment body

    Finds Google Sheets URL in comment body of Reddit post.
    Removes and replaces unwanted characters:
        - "]"
        - "." at end
        - "\\" escaped characters ("_") in URL.

    Args:
        comment (str): Reddit post comment body
        ss_base_url (str): Google Sheets shared base URL

    Returns:
        str: Normalized URL
    """
    ss_url = comment[comment.find(BASE_URL) :].split()[0]

    if "]" in ss_url:
        logger.info(f"Found closing bracket in comment: {ss_url}")
        ss_url = ss_url.split("]")[0]
        logger.info(f"Cleaned SS-URL: {ss_url}")

    if ss_url.endswith("."):
        logger.info(f"Found period at end of URL: {ss_url}")
        ss_url = ss_url[:-1]
        logger.info(f"Cleaned SS-URL: {ss_url}")

    if "\\" in ss_url:
        logger.info(f"Found escaped characters in URL: {ss_url}")
        ss_url = ss_url.replace(")", "").replace("\\", "")
        logger.info(f"Cleaned SS-URL: {ss_url}")

    if ")" in ss_url:
        logger.info(f"Found closing bracket in URL: {ss_url}")
        ss_url = ss_url.replace(")", "")
        logger.info(f"Cleaned SS-URL: {ss_url}")

    return ss_url



def find_sheet_url(post):
    """
    Finds the Google Sheets shared URL in the comment body of a Reddit post.

    Args:
        post (Post): The Reddit post to search.

    Returns:
        str: The normalized Google Sheets shared URL, or None if not found.
    """
    if BASE_URL in post.title:
        return clean_url(post.title)

    if post.selftext and BASE_URL in post.selftext:
        return clean_url(post.selftext)

    if not post.selftext and BASE_URL in post.url:
        return post.url

    for comment in post.comments:
        if BASE_URL in comment.body:
            ss_url = clean_url(comment.body)
            return ss_url

    logger.warning(
        f"find_sheet_url - No sheet found in post: {post.id} - {post.title} - {post.shortlink}"
    )
    return None


def unique_sheets(sheet_posts: list):
    logger.info("Removing duplicate sheets posts...")
    seen_sheet_ids = set()
    unique_sheet_posts = []
    for post in sheet_posts:
        if post.sheet_id is not None and post.sheet_id not in seen_sheet_ids:
            seen_sheet_ids.add(post.sheet_id)
            unique_sheet_posts.append(post)
        else:
            logger.warning(
                f"Skipping duplicate sheet: {post.sheet_id} - {post.author.name}: {post.author.flair} exchanges - {post.shortlink}"
            )
    return unique_sheet_posts


def dedupe_submissions(submissions: list):
    logger.info("Removing duplicate submissions...")
    seen_ids = set()
    unique_submissions = []
    for post in submissions:
        if post.id not in seen_ids:
            seen_ids.add(post.id)
            unique_submissions.append(post)
        else:
            logger.warning(
                f"Skipping duplicate submission: {post.id} - {post.title} - {post.shortlink}"
            )
    return unique_submissions


def download_sheet(sheet_id):
    logger.info(f"Downloading sheet via DFhandler: {sheet_id}")
    sheet_df = DFhandler(sheet_id)

    if isinstance(sheet_df.df, pd.DataFrame):
        return sheet_df


def add_sheet_data(unique_posts: list[SheetPost]):
    items = []
    for idx, sheet in enumerate(unique_posts, start=1):
        logger.info(f"Processing sheet {idx} of {len(unique_posts)}: {sheet.sheet_id}")
        # Check cache before downloading
        if not check_cache(sheet.sheet_id):
            dl_sheet = download_sheet(sheet.sheet_id)
            if dl_sheet:
                logger.info(f"Sheet {sheet.sheet_id} found and processed")
                updated_sheet = SheetPost(
                    id=sheet.id,
                    title=sheet.title,
                    created=sheet.created,
                    shortlink=sheet.shortlink,
                    author=sheet.author,
                    sheet_id=sheet.sheet_id,
                    sheet_url=sheet.sheet_url,
                    sheet_raw=dl_sheet.df.to_json(),
                    sheet_dl_date=datetime.now(),
                )
                items.append(updated_sheet)
    return items


def cache_sheets(items: list[SheetPost]) -> None:
    logger.info("Caching sheets...")
    for item in items:
        cache.hset(hash="sheets", key=f"sheet:{item.sheet_id}", value=item)
        logger.info(f"Caching complete for sheet: {item.sheet_id}")


def check_cache(sheet_id):
    logger.info(f"Checking cache for sheet: {sheet_id}")
    if cache.hexists(hash="sheets", key=f"sheet:{sheet_id}"):
        logger.info(f"Cache found for sheet: {sheet_id}")
        return True
    logger.info(f"Cache **not** found for sheet: {sheet_id}")
    return False


class DFhandler:
    def __init__(self, sheet_id) -> None:
        self.sheet_id = sheet_id
        self.df = None
        logger.info(f"Initializing DFhandler for {self.sheet_id}")
        self.gen_df()

    def gen_df(self):
        """Google Sheet to Pandas DataFrame

        Args:
            sheet_url (Post.sheet): Post sheet URL

        Returns:
            DataFrame: CSV data
        """
        ss_base_url = "https://docs.google.com/spreadsheets/d/"
        export_url = "/export?format=csv&gid=0"
        dl_url = ss_base_url + self.sheet_id + export_url
        logger.info(f"DL-URL: {dl_url}")
        try:
            self.df = pd.read_csv(dl_url)
            self.download_date = datetime.now()
            logger.info(
                {"success": True, "sheet_id": self.sheet_id, "df": self.df.shape}
            )
        except Exception as e:
            self.df = None
            logger.error(
                f"gen_df() {e} - sheet_id: {self.sheet_id} - sheet_url: {dl_url}"
            )
        if self.df is None:
            self.gen_df_gspread()

    def gen_df_gspread(self):
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                logger.info(f"Opening sheet_id: {self.sheet_id}...")
                sheet = gc.open_by_key(self.sheet_id)
                worksheet = sheet.get_worksheet(0)
                self.df = pd.DataFrame(worksheet.get_all_values())
                self.download_date = datetime.now()
                logger.info(
                    {"success": True, "sheet_id": self.sheet_id, "df": self.df.shape}
                )
                break
            except Exception as e:
                if "Quota exceeded" in str(e):
                    retries += 1
                    logger.warning(
                        f"Quota exceeded (attempt {retries}/{max_retries}): {e}\n{self.sheet_id}"
                    )
                    time.sleep(65)
                elif "This operation is not supported" in str(e):
                    logger.error(f"Failed to download sheet. Error: {e}")
                    self.df = None
                    break
                else:
                    retries += 1
                    logger.warning(
                        f"Error downloading sheet (attempt {retries}/{max_retries}): {e} sheet_id: {self.sheet_id}"
                    )
                    if retries < max_retries:
                        time.sleep(5)
                    else:
                        logger.error(
                            f"Failed to download sheet after {max_retries} retries. Error: {e} sheet_id: {self.sheet_id}"
                        )
                        self.df = None
                        break

    @classmethod
    def write_csv(cls, filename=None, include_date=True) -> None:
        """
        Writes the DataFrame to a CSV file.

        Args:
            filename (str, optional): The name of the CSV file to be created.
            If not provided, the sheet_id of the sheet_post is used as the filename.
            Defaults to None.

        Returns:
            None: This function does not return anything.

        Raises:
            None: This function does not raise any exceptions.

        Examples:
            >>> dfhandler = DFhandler(sheet_post)
            >>> dfhandler.df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
            >>> dfhandler.write_csv('my_data.csv')
            >>> # Output: ../docs/spreadsheets/my_data.csv
        """
        try:
            if isinstance(cls.df, pd.DataFrame):
                if filename is None:
                    filename = cls.sheet_id
                if include_date:
                    filename = filename + "_" + datetime.now().strftime("%Y-%m-%d")
                fname = filename + ".csv"
                fileloc = Path("../docs/spreadsheets") / fname
                logger.info(f"CSV written: {fileloc}")
                cls.df.to_csv(fileloc, index=False)
        except Exception as e:
            logger.error(f"Error writing CSV: {e}\n{cls.sheet_id}")

    def __str__(self) -> str:
        if self.df is None:
            return f"DFhandler for sheet ID: {self.sheet_id} (DataFrame not loaded)"
        else:
            return f"""DFhandler for sheet ID: {self.sheet_id}
                    DataFrame Shape: {self.df.shape}
                    DataFrame Columns: {list(self.df.columns)}
                    First 5 Rows:
                    {self.df.head().to_string(index=False, max_rows=5)}
                    """

    # Helper Functions - WIP - Needs testing
    def clean_df(dataframes: list) -> list:
        """Remove None Types from list of DataFrames

        Args:
            dataframes (list): Results from gen_df which may contain None and DataFrames

        Returns:
            list: Clean list of DataFrames
        """
        return [x for x in dataframes if isinstance(x, pd.DataFrame)]


def sheet_query(query="spreadsheet", sort="relevance", time_filter="all", limit=None):
    logger.info(
        f"Searching {limit if limit else 'all'} submissions for {query} with sort={sort} and time_filter={time_filter}"
    )
    ss = list(
        sr.search(query=query, sort=sort, time_filter=time_filter, limit=limit)
    )
    posts = parse_submissions(ss)
    posts.sort(key=lambda x: x.created, reverse=True)
    u_posts = dedupe_submissions(posts)
    u_sheets = unique_sheets(posts)
    dl_sheets = add_sheet_data(u_sheets)
    cache_sheets(dl_sheets)
    logger.info(
        f"""
                Sheet Posts: {len(ss)}
                Unique Sheet Posts: {len(u_posts)}
                Duplicates Sheet Posts Removed: {len(ss) - len(u_posts)}
                Unique Sheets: {len(u_sheets)}
                Sheets Downloaded: {len(dl_sheets)}
                """
    )
    return {
        "search_results": ss,
        "unique_sheet_posts": u_posts,
        "posts": posts,
        "unique_sheets": u_sheets,
        "sheets_downloaded": dl_sheets,
    }


if __name__ == "__main__":
    # TESTING
    # sheet_query()
    # sheet_query(sort="new", time_filter="week")
    # sheet_query(sort="hot")
    # sheet_query(sort="top")
    # sheet_query(sort="comments")
    # headers = header_parse()  # Moved to transformations.py