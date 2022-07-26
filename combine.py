from heapq import merge
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

    # now we have to remove dupliate entries. But our special structure makes the normal drop_duplicates crash
    # So we convert the mergedIds to a string, drop the duplicates baseed on that and reconvert them to dicts after that
    ledger_data["mergedIdentityIds"] = ledger_data.apply(
        lambda row: " ".join(row["mergedIdentityIds"]), axis=1
    )
    ledger_data = ledger_data.drop_duplicates(subset=["mergedIdentityIds"])
    ledger_data["mergedIdentityIds"] = ledger_data.apply(
        lambda row: row["mergedIdentityIds"].split(" "), axis=1
    )

    # Now we can merge the grain with the ledger. For this, we move the sourcecred ID and name saved under "identity" into a new column

    ledger_data["id"] = ledger_data.apply(lambda row: row["identity"]["id"], axis=1)
    ledger_data["name"] = ledger_data.apply(lambda row: row["identity"]["name"], axis=1)

    merge1 = grain_distributions.merge(ledger_data, on=["id", "name"], how="left")
    # merge1 = cred_distributions.merge(ledger_data, on=["id", "name"], how="left")
    # print(merge1.head())
    # merge1.to_csv("./testCredMerge.csv")

    # We also create separate columns for the different service IDs to make them easily accesible
    merge1["discord_id"] = merge1.apply(
        lambda row: find_linked_id("discord", row["identity"]["aliases"]), axis=1
    )
    merge1["github_id"] = merge1.apply(
        lambda row: find_linked_id("github", row["identity"]["aliases"]), axis=1
    )

    merge1["discourse_id"] = merge1.apply(
        lambda row: find_linked_id("discourse", row["identity"]["aliases"]), axis=1
    )

    merge1["discord_username"] = merge1.apply(
        lambda row: find_discord_name(row["identity"]["aliases"]), axis=1
    )

    # Now we try to merge it with the accounts from the sourcecred mass activation. Since data is inconsistent, we need to check what matches we can find and fill out accordingly

    # we prepare the new dataframe, merge2 adding the columns of activated users. We choos to take the discord username from the activated lsit since that one is more recent
    merge2 = merge1.copy()
    merge2["address"] = "MISSING"

    # we transform the type of discordId for easy comparison
    user_reference = activated_users.copy()
    user_reference["discordId"] = user_reference["discordId"].astype(str)
    user_reference.fillna("MISSING", inplace=True)

    cols_to_compare = [
        ("discord_id", "discordId"),
        ("github_id", "github"),
        ("discourse_id", "discourse"),
        ("address", "address"),
        ("discord_username", "username"),
    ]

    # NOTE we give preference to the "activated user" list, since this one is more recent
    merge2 = merge2.apply(
        lambda row: fill_out_matches(row, user_reference, cols_to_compare),
        axis=1,
    )

    # Pretty up data for saving
    merge2.rename(columns={"id": "sourcecred_id"}, inplace=True)

    move_front = [
        "name",
        "discord_username",
        "address",
        "github_id",
        "discourse_id",
        "discord_id",
        "sourcecred_id",
    ]
    merge2 = merge2[move_front + [x for x in merge2.columns if x not in move_front]]

    print(merge2.head())

    merge2.to_csv("./clean_data/merged_grain_table.csv")


def find_linked_id(linked_service, alias_list):
    for elem in alias_list:
        if elem["address"][2] == linked_service:
            return elem["address"][5]

    return "MISSING"


def find_discord_name(alias_list):
    for elem in alias_list:
        data = elem["description"].split("/")
        if data[0] == "discord":
            return data[1]

    return "MISSING"


def fill_out_matches(df_Row, reference_DF, check_columns):

    matchingRow = pd.Series(dtype="str")

    # finds the first row containing matching data
    for orig, ref in check_columns:
        if df_Row[orig] != "MISSING":
            colId = df_Row[orig]
            matchingRow = reference_DF[reference_DF[ref] == colId]
            if not matchingRow.empty:
                break

    if not matchingRow.empty:
        matchingRow = matchingRow.iloc[0].squeeze()
        for orig, ref in check_columns:
            if matchingRow[ref] != "MISSING":
                df_Row[orig] = matchingRow[ref]

    return df_Row


if __name__ == "__main__":
    combine()
