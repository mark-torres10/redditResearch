# Studying online outrage on Reddit

## Purpose


## Logic

## Setup

### Package and environment setup

#### Python setup
Install Python. This project uses Python3.9; the code may or may not work depending on the Python version (the packages in the `requirements.txt` file assume Python3.9 and some packages, such as Tensorflow and numpy, are very particular to their Python version).

I also personally use Anaconda to do package management, since this is one way to conveniently create multiple virtual environments using multiple Python versions, but there are multiple ways to create virtual environments. See [the Conda docs](https://conda.io/projects/conda/en/latest/user-guide/getting-started.html) for more information.

#### Reddit app setup
In the root directory, not the `src` directory, create a .env file. In that .env file, insert the credentials for your Reddit app, as `REDDIT_CLIENT_ID`, `REDDIT_SECRET`, `REDDIT_REDIRECT_URI`, `REDDIT_USERNAME`, and `REDDIT_PASSWORD`. Create a new refresh token (see `lib/get_refresh_token.py`), then add that as `REFRESH_TOKEN` to the .env file.

