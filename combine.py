from re import sub
import pandas as pd
import ast
import csv, json

# Import all three datasets

activated_users = pd.read_csv("./clean_data/activatedUsers.csv")
print(activated_users.head())


grain_distributions = pd.read_csv("./clean_data/grainDistributions.csv")
print(grain_distributions.head())


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

print(ledger_data.head())

# Now we can merge the grain with the ledger. For this, we move the sourcecred ID and name saved under "identity" into a new column


ledger_data["id"] = ledger_data.apply(lambda row: row["identity"]["id"], axis=1)
ledger_data["name"] = ledger_data.apply(lambda row: row["identity"]["name"], axis=1)

merge1 = grain_distributions.merge(ledger_data, on=["id", "name"], how="left")
print(merge1.head())


# We create separate columns for the different service IDs


def find_linked_id(linked_service, alias_list):
    for elem in alias_list:
        if elem["address"][2] == linked_service:
            return elem["address"][5]
    # return f"no {linked_service} ID"
    return "NONE"


merge1["discord_id"] = merge1.apply(
    lambda row: find_linked_id("discord", row["identity"]["aliases"]), axis=1
)
merge1["github_id"] = merge1.apply(
    lambda row: find_linked_id("github", row["identity"]["aliases"]), axis=1
)

merge1["discourse_id"] = merge1.apply(
    lambda row: find_linked_id("discourse", row["identity"]["aliases"]), axis=1
)

print(merge1.head())


merge1.to_csv("first_merge.csv")


# Now we try to merge it with the accounts from the sourcecred mass activation. Since data is inconsistent, we need to check what matches we can find and fill out accordingly


def fill_out_match(df_Row, reference_DF):
    discId = df_Row["discord_id"]
    if df_Row["discord_id"] != "NONE":
        print(discId)
        matchingRow = reference_DF.loc[reference_DF["discordId"] == str(discId)]
        # print(matchingRow)
        return pd.concat([pd.DataFrame(df_Row), pd.DataFrame(matchingRow)])


merge2 = merge1.apply(lambda row: fill_out_match(row, activated_users), axis=1)

print(merge2.head())


# merge1 = grain.merge(ledgerDF, on=["id", "name"], how="left")
# print(merge1.head())

# #mergeTest = merge1[["id", "name_x", "name_y"]].copy()

# # print(mergeTest.head())

# #mergeTest.to_csv("TESTMERGE.csv")
