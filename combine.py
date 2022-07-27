# ======= DEPRECATED FOR NOW =======
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#


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

    merge1 = grain_distributions.merge(ledger_data, on=["id", "name"], how="outer")
    # merge1 = merge1.merge(cred_distributions, on=["id", "name"], how="outer")
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

    merge1 = merge1.merge(cred_distributions, on=["id", "name"], how="outer")

    # Now we try to merge it with the accounts from the sourcecred mass activation. Since data is inconsistent, we need to check what matches we can find and fill out accordingly

    # we prepare the new dataframe, merge2 adding the columns of activated users. We choose to take the discord username from the activated list since that one is more recent
    merge2 = merge1.copy()
    merge2["address"] = "missing"

    # we transform the type of discordId for easy comparison
    user_reference = activated_users.copy()
    user_reference["discordId"] = user_reference["discordId"].astype(str)
    user_reference.fillna("missing", inplace=True)

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

    merge2.to_csv("./clean_data/merged_mega_table.csv")

    # TODO NUEVO ENFOQUE:
    # Hacer lo que hemos hecho con el ledger con la tabla del cred:
    # recorrerla nombre a nombre e intentar dar con matches. Primero ID luego github etc
    # Pasos:
    #       limpiar nombres: lowercase, quitar "-discourse", "-github" etc. Tenemos las ids aun

    manual_select = [
        "name",
        "discord_username",
        "github_id",
        "discourse_id",
        "sourcecred_id",
        "address",
        #        "discord_id",
        #       "totalGrainPaid",
        "totalCred",
    ]

    conflict_df = merge2[manual_select].copy()

    conflict_df["name"] = (
        conflict_df["name"].apply(lambda x: cleanup_name(x)).str.lower()
    )

    conflict_df["name"] = conflict_df["name"].str.lower()
    conflict_df["github_id"] = conflict_df["github_id"].str.lower()
    conflict_df["discourse_id"] = conflict_df["discourse_id"].str.lower()
    conflict_df = conflict_df.sort_values(by=["name"])
    conflict_df.fillna("missing", inplace=True)

    test = pd.DataFrame(columns=conflict_df.keys())
    test["name"] = pd.Series(conflict_df["name"].unique())
    test.to_csv("./test_names.csv", index=False)
    conflict_df["totalCred"] = conflict_df["totalCred"].replace(
        [numpy.NaN], 0
    )  # we want to keep the ones with missing score in case we can match them
    conflict_df["totalCred"] = conflict_df["totalCred"].replace(["missing"], 0)
    print(test.head())
    test = test.apply(lambda x: merge_into_one(x, conflict_df), axis=1)
    test["totalCred"] = test["totalCred"].replace(["missing"], 0.01)
    test = test.loc[test["totalCred"] >= 0.01]
    test.to_csv("./test_table.csv", index=False)

    # filter out cred below 0.01 and make some readability changes

    conflict_df = conflict_df.loc[conflict_df["totalCred"] >= 0.01]

    conflict_df = conflict_df.replace(["missing"], "")
    conflict_df.to_csv("./clean_data/summary_table.csv", index=False)


def find_linked_id(linked_service, alias_list):

    for elem in alias_list:

        if elem["address"][2] == linked_service:
            return elem["address"][5]

    return "missing"


def find_discord_name(alias_list):

    for elem in alias_list:
        data = elem["description"].split("/")
        if data[0] == "discord":
            return data[1]

    return "missing"


def fill_out_matches(df_Row, reference_DF, check_columns):

    matchingRow = pd.Series(dtype="str")

    # finds the first row containing matching data
    for orig, ref in check_columns:
        if df_Row[orig] != "missing":
            colId = df_Row[orig]
            matchingRow = reference_DF[reference_DF[ref] == colId]
            if not matchingRow.empty:
                # print(matchingRow)
                break

    if not matchingRow.empty:
        matchingRow = matchingRow.iloc[0].squeeze()
        for orig, ref in check_columns:
            if matchingRow[ref] != "missing":
                df_Row[orig] = matchingRow[ref]

    return df_Row


def merge_into_one(row, ref_df):
    val = row["name"]

    res = ref_df[ref_df.isin([val]).any(axis=1)]

    if val == "efra":
        print(res)
        print(type(res["totalCred"].iloc[0]))
    for col in row.keys():
        # print(res[col])
        row[col] = squeeze(res[col])

    if val == "efra":
        print(row)
    return row


def squeeze(col):
    if type(col.iloc[0]) == numpy.float64:
        return sum(col)
    else:
        col = col.unique()
        # print(col)
        # print(type(col))
        col = numpy.delete(col, numpy.where(col == "missing"))
        # print(col)
        if not col.any():
            return "missing"
        else:
            return col[0]


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
    print("=============DEPRECATED FOR NOW=============")
    # combine()
