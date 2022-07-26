import pandas as pd
import numpy
import ast
import csv, json


# Highligt cred/grain issue
grain_distributions = pd.read_csv("./clean_data/grainDistributions.csv")
grain_distributions = grain_distributions[["id", "name", "totalGrainPaid"]]
# print(grain_distributions.head())

cred_distributions = pd.read_csv("./clean_data/credDistributions.csv")
cred_distributions = cred_distributions[["id", "name", "totalCred"]]
# print(cred_distributions.head())

merge1 = grain_distributions.merge(cred_distributions, on=["id", "name"], how="outer")
merge1 = merge1.sort_values(by=["totalCred"], ascending=False)
merge1.to_csv("./cred_grain_issue.csv")
