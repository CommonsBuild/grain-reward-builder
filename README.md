# pollen-onboarding

A script to add, merge and activate new users in the Pollen instance.

## Developer setup

### Install dependencies

`npm install`, `npm install -D`

### Add env variables

Copy the content of your `.env.sample` file into a new `.env` file and add the corresponding variables:

- `MONGODB_URI` = Your MongoDB connection URI.
- `GH_API_TOKEN` = GitHub token with commit permissions to the repo.
- `REPO` = The repo you want to commit to.
- `BRANCH` = The branch you want to commit to.
- `REPO_AND_BRANCH` = The repo and branch of your sourcecred instance. In this case it's https://raw.githubusercontent.com/1Hive/pollen/gh-pages/

### Run the script!

- `npm start` will execute the script in production mode.
- `npm run dev` will execute the script in developer mode, with hot reloading.


### For the python scripts
Once you have the files in the raw_data folder after running the js scripts, you can run clean_data.py and new_combine.py (in this order). It is recommended you set up a virtual environment and install the required libraries with ```pip install -r requirements.txt```
