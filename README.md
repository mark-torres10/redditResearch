# Studying online outrage on Reddit

## Purpose

## Logic

## Setup

## Package and environment setup

### Python setup
Install Python. This project uses Python3.9; the code may or may not work depending on the Python version (the packages in the `requirements.txt` file assume Python3.9 and some packages, such as Tensorflow and numpy, are very particular to their Python version).

I also personally use Anaconda to do package management, since this is one way to conveniently create multiple virtual environments using multiple Python versions, but there are multiple ways to create virtual environments. See [the Conda docs](https://conda.io/projects/conda/en/latest/user-guide/getting-started.html) for more information.

### Reddit app setup
In the root directory, not the `src` directory, create a .env file. In that .env file, insert the credentials for your Reddit app, as `REDDIT_CLIENT_ID`, `REDDIT_SECRET`, `REDDIT_REDIRECT_URI`, `REDDIT_USERNAME`, and `REDDIT_PASSWORD`. Create a new refresh token (see `lib/get_refresh_token.py`), then add that as `REFRESH_TOKEN` to the .env file.



### Install Postgres
This project makes use of Postgres as its DB. To start, install Postgres for your system (https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).
## How to run
*For the latest information on how to run this code, check out the [runbook](https://torresmark.notion.site/Runbook-af1806fe333743bbb4c9932b0d3842f4?pvs=4) for this code.

## Data schemas
*For the latest information on the data schemas, check out [this description of the data schemas at each stage of the pipeline](https://torresmark.notion.site/Schemas-1537156c483e47d292a40bc81b70fd8f?pvs=4).