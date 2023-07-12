# data
import pandas as pd
import numpy as np

# drawing
import matplotlib.pyplot as plt
import seaborn as sns

# predictions
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression

# load transport data
transport_raw = pd.read_table("tran_hv_pstra.tsv")  # transport

# copy original
transport_clean = pd.DataFrame(transport_raw)

# extract country data
transport_clean[["unit", "country"]] = transport_raw["unit,geo\\time"].str.split(
    ",", expand=True
)
transport_clean = transport_clean.drop(["unit,geo\\time"], axis=1)

# extract the names of year columns
transport_year_col_names = transport_clean.columns.tolist()
transport_year_col_names.remove("country")
transport_year_col_names.remove("unit")

# extract the percent number from data
transport_clean["2015_percent"] = transport_clean["2015 "].astype(str)
transport_clean = transport_clean.drop("2015 ", axis=1)
transport_year_col_names.remove("2015 ")

for col in transport_year_col_names:
    newcol = col.rstrip()
    split = transport_clean[col].str.split(" ", expand=True)
    transport_clean[[newcol + "_percent", newcol + "_letter"]] = split
    transport_clean = transport_clean.drop(col, axis=1)

# select column names again
transport_year_col_names = [col.rstrip() for col in transport_year_col_names]
transport_year_col_names.append("2015")
transport_year_col_names = sorted(transport_year_col_names)

# sort the year columns
transport_clean = transport_clean.reindex(sorted(transport_clean.columns), axis=1)

# drop the letter columns (unused columns)
transport_clean = transport_clean.loc[
    :, ~transport_clean.columns.str.endswith("_letter")
]

# remove percent postfix because letters were not used
transport_clean = transport_clean.rename(
    columns=lambda x: x.replace("_percent", "") if "_percent" in x else x
)

# add transport_ prefix
transport_clean = transport_clean.rename(
    columns={
        col: f"transport_{col}"
        for col in transport_clean.columns
        if col not in ["country", "unit"]
    }
)

# remove unit column (all are the same)
transport_clean = transport_clean.drop("unit", axis=1)

# put country first (cosmetic)
last_column = transport_clean.pop(transport_clean.columns[-1])
transport_clean.insert(0, last_column.name, last_column)

# make year columns float
transport_clean[["transport_" + y for y in transport_year_col_names]] = transport_clean[
    ["transport_" + y for y in transport_year_col_names]
].apply(lambda x: pd.to_numeric(x, errors="coerce"))


# load pollution data
pollution_raw = pd.read_table("urb_percep.tsv")  # air pollution

# copy original
pollution_clean = pd.DataFrame(pollution_raw)

# extract city data
pollution_clean[["indic_ur", "unit", "city"]] = pollution_clean[
    "indic_ur,unit,cities\\time"
].str.split(",", expand=True)
pollution_clean = pollution_clean.drop(["indic_ur,unit,cities\\time"], axis=1)

# drop unused
pollution_clean = pollution_clean.drop(["unit", "indic_ur"], axis=1)

# extract country
pollution_clean["country"] = pollution_clean["city"].str[0:2]

# sort the year columns
pollution_clean = pollution_clean.reindex(sorted(pollution_clean.columns), axis=1)

# remove spaces from year column names
pollution_clean.columns = [col.rstrip() for col in pollution_clean.columns]

# get year column names
pollution_year_col_names = pollution_clean.columns.tolist()
pollution_year_col_names.remove("city")
pollution_year_col_names.remove("country")

# change year columns to float
pollution_clean[pollution_year_col_names] = pollution_clean[
    pollution_year_col_names
].apply(lambda x: pd.to_numeric(x, errors="coerce"))

# drop rows with NaN
pollution_clean = pollution_clean.dropna(subset=pollution_year_col_names)

# put _pollution prefix
pollution_clean = pollution_clean.rename(
    columns={
        col: f"pollution_{col}"
        for col in pollution_clean.columns
        if col not in ["country", "city"]
    }
)

# merge on country column
merged = pd.merge(transport_clean, pollution_clean, on="country", how="right")

# drop transport columns which year doesn't fit pollution data
merged = merged.drop(
    [
        "transport_" + col
        for col in transport_year_col_names
        if "pollution_" + col not in merged.columns
        and "transport_" + col not in ["transport_2021"]
    ],
    axis=1,
)

merged = merged.drop_duplicates(subset=["city"], ignore_index=True)

# save to csv
merged.to_csv("merged.csv")

# create new features dataframe for ML
columns = ["country", "city", "transport", "pollution", "year"]
features = pd.DataFrame(columns=columns)

# create transport and pollution features based on the transport_YYYY and pollution_YYYY combined
transports = pd.DataFrame()
pollutions = pd.DataFrame()

# extract transport and assign correspoding years
for col in merged.columns:
    if col.startswith("transport_"):
        year = col.split("_")[1]
        polcol = None
        new_rows = merged[[col, "country", "city"]].rename(columns={col: "transport"})
        new_rows["year"] = year
        transports = pd.concat([transports, new_rows], ignore_index=True)

# extract pollution and assign corresponding years
for col in merged.columns:
    if col.startswith("pollution_"):
        year = col.split("_")[1]
        polcol = None
        new_rows = merged[[col, "country", "city"]].rename(columns={col: "pollution"})
        new_rows["year"] = year
        pollutions = pd.concat([pollutions, new_rows], ignore_index=True)

# merge the transport and pollution into features dataframe
features = pd.merge(transports, pollutions, on=["city", "year", "country"], how="left")

# sort by city and year for better view
features = features.sort_values(["city", "year"])
features = features.reset_index(drop=True)

# reorganize the column names
features = features[["country", "city", "transport", "pollution", "year"]]

# save the features to csv
features.to_csv("features.csv")
