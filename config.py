import tomllib
from dataclasses import dataclass


@dataclass
class Config:
    databases: dict
    logging: dict
    praw: dict


def get_config():
    """
    Returns the configuration object.

    Returns:
        Config: The configuration object.
    """
    with open("./config.toml", "rb") as f:
        config = tomllib.load(f)
    return Config(**config)
