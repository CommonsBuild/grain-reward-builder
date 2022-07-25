import pandas as pd
import ast
import csv

# This script takes the raw data pulled from sourcecred an cleans it up to make it readable:
def clean_data():

    # ==== ACTIVATED USER ACCOUNTS ====
    # This table contains the users who took part in the "Sourcecred Mass activation", linking different accounts together
    raw_read = pd.read_csv("./raw_data/activatedUsers.csv")

    activatedSeriesArray = []
    for index, entry in raw_read.iterrows():
        activatedSeriesArray.append(pd.Series(ast.literal_eval(entry["_doc"])))

    activated_users = pd.DataFrame(activatedSeriesArray)

    print(activated_users.head())
    activated_users.to_csv("./clean_data/activatedUsers.csv", index=False)

    # ==== GRAIN DISTRIBUTIONS ====
    # This table contains the grain distributions exported from the ledger, while discarding all empty distributions

    grain = pd.read_csv("./raw_data/grain.csv")
    grain = grain.loc[:, (grain != 0).any(axis=0)]
    print(grain.head())
    grain.to_csv("./clean_data/grainDistributions.csv", index=False)

    # ==== ACCOUNTS FROM THE LEDGER ====
    # This table contains the accounts as they are stored in the Sourcecred ledger
    # I've chosen to keep absolutely everything for now ti be sure, but for further use all those Dataframes inside of dataframes will need to be trimmed. To avoid issues, I've replaced the \u0000 caracters in the aliases with backslashes

    # Convert the CSV file to JSON
    jsonData = []
    with open("./raw_data/ledgerAccounts.csv", encoding="utf-8") as csvf:
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

    ledgerDF = pd.DataFrame(jsonData)

    # Now we have to sanitize the strings stored the "identity" dict. For some reason, these strings are riddled with the null character "\u0000"
    # To make future retrieval easy, we split the strings by this null character into an array
    for index, row in ledgerDF.iterrows():
        row["identity"]["address"] = row["identity"]["address"].split("\u0000")
        for alias in row["identity"]["aliases"]:
            # print(len(row["identity"]["aliases"]))
            for elem in alias:
                if elem == "address":
                    alias[elem] = alias[elem].split("\u0000")
                # print(alias[elem])

    print(ledgerDF.head())
    ledgerDF.to_csv("./clean_data/ledgerData.csv", index=False)


if __name__ == "__main__":

    clean_data()
