# IHATE ⚡ SHEETS

A project for users who prefer alternatives to viewing traditional spreadsheets in For Sale or Trade (FSOT) subreddits.

**ihate_sheets** aims to provide an alternative way to manage, analyze, and view sheets data.

## Overview

**ihate_sheets** looks to extract that data for analyzation but mostly to output it to a better format. I hate viewing FSOT threads and bouncing back and forth for pictures, trying to figure out some user defined look up table, or any other "unique" way the author put a sheet together. Don't even get me started on the sick-o's who paste spreadsheets into docs 🫣.

### Status

This is really just a learning and fun project. I wanted to learn about some new tools and tried implementing them in this project, which is still very much a "WIP" (work in progress). And very likely broken, but working on my machine 😉.

`2025-06-09`: This was taken from a larger project and is still being converted to a stand alone application.

Technologies:

- [Redis](https://redis.io/docs/latest/)
- [msgspec](https://jcristharif.com/msgspec/)
- [PRAW](https://praw.readthedocs.io/en/stable/index.html)
- [MongoDB](https://www.mongodb.com/)

## Features

- Searching subreddits for Sheets
- Downloading and saving submissions to a `Redis` database (including raw sheets data)
- Exploring seller flair and transactions (saved to a `Mongo DB`)
- Exploring "ISO" (in search of) submissions

## Set up

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/ihate_sheets.git
    cd ihate_sheets
    ```

2. Install dependencies:
    
    ```bash
    # Example for Python
    pip install -r requirements.txt
    ```

3. Run the tool:
    
    ```bash
    python app.py
    ```

## License

This project is licensed under the MIT License.
