import praw
from config import get_config

# Open settings
CONFIG = get_config()

# Init constants
CLIENT_ID = CONFIG.praw["credentials"]["client"]
CLIENT_SECRET = CONFIG.praw["credentials"]["secret"]
USERNAME = CONFIG.praw["credentials"]["user"]
APPNAME = CONFIG.praw["app"]["name"]
PASSWORD = CONFIG.praw["credentials"]["password"]
SUBREDDIT = CONFIG.praw["subreddit"]

# Init reddit
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    password=PASSWORD,
    user_agent=USERNAME + " " + APPNAME,
    username=USERNAME,
)

assert reddit.user.me() == USERNAME, f"Connection with user: {USERNAME}, failed"

def user_exists(post):
    try:
        reddit.redditor(post.author.name).id
    except NotFound:
        return None
    return True
