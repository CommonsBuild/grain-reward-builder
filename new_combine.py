from hashlib import new
from heapq import merge
from tokenize import Number
import pandas as pd
import numpy
import ast
import csv, json


def combine():
    # Import all three datasets

    activated_users = pd.read_csv("./clean_data/activatedUsers.csv")
    # print(activated_users.head())

    grain_distributions = pd.read_csv("./clean_data/grainDistributions.csv")
    # print(grain_distributions.head())

    cred_distributions = pd.read_csv("./clean_data/credDistributions.csv")
    # print(cred_distributions.head())

    # sadly, to load it we have to do the same as in cleaning again
    jsonData = []
    with open("./clean_data/ledgerData.csv", encoding="utf-8") as csvf:
        csvReader = csv.DictReader(csvf)

        # Build each row reading the stored strings as dict
        # and add it to data
        for row in csvReader:
            buildRow = {}
            for elem in row:
                try:
                    buf = ast.literal_eval(row[elem])
                except:
                    # for non activated accounts the export is saved empty instead of 0
                    # we correct this here.
                    buf = "0"

                buildRow[elem] = buf
            jsonData.append(buildRow)

    ledger_data = pd.DataFrame(jsonData)

    # New clean slate attempt

    # clean the cred:
    # remove -dicourse etc, add cred, append ids
    cred_copy = cred_distributions[["id", "name", "totalCred"]].copy()
    cred_copy["name"] = (
        cred_distributions["name"].apply(lambda x: cleanup_name(x)).str.lower()
    )
    clean_names = pd.Series(cred_copy["name"].unique())
    # print(clean_names)

    clean_cred = pd.DataFrame(columns=["id", "name", "totalCred"])
    clean_cred = clean_names.apply(lambda x: fill_and_merge(x, cred_copy))
    print(clean_cred.head())
    clean_cred["totalCred"] = [sum(a) for a in clean_cred["totalCred"]]
    print(clean_cred.head())

    # try to match cred with any acivated user
    select_col = [
        "username",
        "address",
        "github",
        "discourse",
        "discordId",
    ]
    active_copy = activated_users[select_col].copy()
    active_copy.rename(columns={"username": "discord"}, inplace=True)
    active_copy["discord"] = active_copy["discord"].str.lower()
    active_copy["github"] = active_copy["github"].str.lower()
    active_copy["discourse"] = active_copy["discourse"].str.lower()
    print(active_copy.head())

    merged_result = pd.DataFrame(columns=["id", "name", "totalCred", select_col])
    merged_result = clean_cred.apply(lambda x: match_entries(x, active_copy), axis=1)
    print(merged_result.head())

    col_order = [
        "name",
        "totalCred",
        "github",
        "discourse",
        "address",
        "discord",
        "discordId",
        "id",
    ]

    merged_result = merged_result[col_order]

    # filter out cred below 0.01
    merged_result = merged_result.loc[merged_result["totalCred"] >= 0.01]

    merged_result.to_csv("./clean_data/merged_cred.csv", index=False)


def fill_and_merge(name, data):
    res = data[data.isin([name]).any(axis=1)]
    newRow = pd.Series(dtype=object)
    newRow["name"] = name
    newRow["id"] = res["id"].to_list()
    newRow["totalCred"] = res["totalCred"].to_list()
    # print(newRow)
    return newRow


def match_entries(row, data):
    val = row["name"]
    res = data[data.isin([val]).any(axis=1)]
    newRow = pd.Series(dtype=object)
    newRow["name"] = row["name"]
    newRow["id"] = row["id"]
    newRow["totalCred"] = row["totalCred"]
    if not res.empty:
        res = res.squeeze()
        newRow["discord"] = res["discord"]
        newRow["address"] = res["address"]
        newRow["github"] = res["github"]
        newRow["discourse"] = res["discourse"]
        newRow["discordId"] = res["discordId"]

        # print("NEWROW:")
        # print(newRow)

    # print(newRow)
    return newRow.squeeze()


def cleanup_name(name):
    patterns = ["-discourse", "-github", "-discord"]
    name = str(name)
    for pat in patterns:
        if len(name) < len(pat):
            continue

        length = len(pat) * -1
        end = name[length:]
        if str(end) == pat:
            # print(str(name[0:length]))
            return str(name[0:length])

    return name


if __name__ == "__main__":
    combine()
