from reddit import reddit
from rich import print as rprint
from models import Author, Post, utc_to_local
import logging
from rich.logging import RichHandler
from sheets import sheet_query
from database import check_for_update

# Init logging
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler("./logs/app.log")
file_handler.setFormatter(file_formatter)

logging.basicConfig(
    level="INFO",
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True), file_handler],
)

logger = logging.getLogger("rich")


# ISO posts
def new_iso(subreddit, limit: int = 25):
    """New In Search Of Posts

    Args:
        subreddit (reddit): Subreddit name

    Returns:
        List: ISO post details
    """
    logger.info(f"Getting {limit} new ISO posts")
    iso_posts = []
    for post in subreddit.new(limit=limit):
        if post.link_flair_text == "In Search Of":
            local_utc = utc_to_local(post.created_utc).strftime('%Y-%m-%d %H:%M')
            logger.info(f"POST: {local_utc} - {post.id} - {post.title[:50]} - {post.shortlink}")
            author = (
                Author(
                    name=post.author.name,
                    author_flair_text=post.author_flair_text,
                )
                if post.author
                else None
            )
            item = Post(
                id=post.id,
                title=post.title,
                created=post.created_utc,
                shortlink=post.shortlink,
                author=author,
            )
            iso_posts.append(item)

    return iso_posts


def new_submission_stream(subreddit):
    for post in subreddit.stream.submissions():
        if post.link_flair_text == "In Search Of":
            local_utc = utc_to_local(post.created_utc).strftime('%Y-%m-%d %H:%M')
            logger.info(f"ISO POST: {local_utc} - {post.id} - {post.title[:50]} - {post.shortlink}")
            author = (
                Author(
                    name=post.author.name,
                    author_flair_text=post.author_flair_text,
                )
                if post.author
                else None
            )
            item = Post(
                id=post.id,
                title=post.title,
                created=post.created_utc,
                shortlink=post.shortlink,
                author=author,
            )
        else:
            logger.info(f"POST: {local_utc} - {post.id} - {post.title[:50]} - {post.shortlink}")


if __name__ == "__main__":
    from config import get_config
    CONFIG = get_config()

    # Set subreddit
    subreddit = reddit.subreddit(CONFIG.praw["subreddit"])
    db_json = check_for_update()
    sheets = sheet_query(sort="new", time_filter="week")
    iso = new_iso(subreddit)
