from typing import final
import pandas as pd
import numpy


def generate_aragon():

    address_df = pd.read_csv("./praise_crosscheck_merged_cred.csv")
    # print(address_df.head())
    merge_df = address_df[["name", "address"]].copy()

    final_allocs = pd.read_csv(
        "./SourcecredSimWithFinalData Cred_CommonsBuild_tec-sourcecred_gh-pages.csv"
    )
    # print(grain_distributions.head())
    final_allocs.rename(columns={"10% vested per round": "vested_amount"}, inplace=True)

    final_merge = final_allocs.merge(merge_df, on=["name"], how="left")
    final_merge = final_merge[["name", "address", "vested_amount"]].copy()

    final_merge[["name", "address", "vested_amount"]].to_csv(
        "./sourcecred.csv", index=False
    )

    only_activated = final_merge.dropna()

    arag_squeeze = (
        only_activated[["address", "vested_amount"]]
        .copy()
        .groupby(["address"], as_index=False)
        .sum()
    )
    arag_squeeze = arag_squeeze.sort_values(by="vested_amount", ascending=False)
    arag_squeeze["token"] = "TEC"
    print(arag_squeeze.head())

    arag_squeeze.to_csv(
        "./sourcecred_aragon_distribution.csv", index=False, header=False
    )

    only_activated[["name", "vested_amount"]].to_csv(
        "./sourcecred_forum_table.csv", index=False
    )


if __name__ == "__main__":
    generate_aragon()
