import csv
import os
from datetime import date, datetime, timedelta
from time import sleep

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tqdm import tqdm

slack_api_token = os.environ["SLACK_API_TOKEN"]
slack_client = WebClient(token=slack_api_token)

langs = {
    "Bengali": "https://docs.google.com/spreadsheets/d/1VnM6uCL6nkkGBsYZ49z5a7JCJ3EPMf83P7LQs8NCQMc/edit#gid=728021100",
    "Odia": "https://docs.google.com/spreadsheets/d/1w6RBlyvFfnoJut7fYH_VlQUATiwKDY6DchZE0nMIDKQ/edit#gid=547042898",
    "Hindi": "https://docs.google.com/spreadsheets/d/1B_jJtEs2kIuq41QWtqTAQymMfUp5aZXenv8BVqNT2wM/edit#gid=67314384",
    "Assamese": "https://docs.google.com/spreadsheets/d/17wpO0wZZt5frjkY3-J7KZ5IotW9BQKLpeJuoPTdxMs0/edit#gid=682966645",
    "Bodo": "https://docs.google.com/spreadsheets/d/1uSL7hlJZNbigam4FZcS8C5fYIDYJvgjJTJ6eatx-f3A/edit#gid=161684224",
    "Konkani": "https://docs.google.com/spreadsheets/d/1n7ouVdOnPKGXlT_5zz8KJGecSxqTET08QdmM_Z42IDY/edit#gid=37977472",
    "Kashmiri": "https://docs.google.com/spreadsheets/d/1kxrsoWipfCF85ddp5-ztjxB6dlG92vTR0TA-eUO59Qs/edit#gid=614108795",
    "Maithili": "https://docs.google.com/spreadsheets/d/1ttBnuf5CjWKOkB_hwWlAEP-jqLLPZegFGrGhD9-ZrnE/edit#gid=196609157",
    "Sanskrit": "https://docs.google.com/spreadsheets/d/130B1y0zjjihSoVAqKfPNvbvQFOUrHfDvLiZjB-PrIas/edit#gid=1173332346",
}


creds = ServiceAccountCredentials.from_json_keyfile_name(
    "credentials.json",
    ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"],
)
client = gspread.authorize(creds)

ai4b_langs = ["Bengali", "Odia", "Hindi"]
errors = {}

current_date = datetime.now()
yesterday = current_date - timedelta(days=1)
if yesterday.weekday() == 6:  # Sunday corresponds to 6 in the weekday() function
    # Subtract one more day for Saturday
    yesterday = yesterday - timedelta(days=1)

yesterday = yesterday.strftime("%d/%m/%y")

for lang, url in langs.items():
    sheet = client.open_by_url(url)
    current_date = date(2023, 9, 1)
    last_date = datetime.today() - timedelta(days=1)
    df = pd.DataFrame()
    sheets = sheet.worksheets()
    for wk in tqdm(sheets, desc=lang):
        sleep(2)
        os.makedirs(f"/home/sai/work/attendance/lrm_scripts/{lang}", exist_ok=True)
        if "sheet" in str(wk.title).lower() or "summary" in str(wk.title).lower():
            continue

        data = wk.get_values()

        if not data:
            continue

        if "Date of QA check" not in data[0]:
            continue

        if "Name" not in data[0]:
            continue

        if "QA Done by" not in data[0]:
            continue

        if current_date <= date.today():
            if not data:
                continue

            with open(
                f'/home/sai/work/attendance/lrm_scripts/{lang}/{current_date.strftime("%d_%m_%y")}.csv',
                "w+",
                newline="",
            ) as file:
                writer = csv.writer(file)
                for row in data:
                    writer.writerow(row)

            tdf = pd.read_csv(
                f'/home/sai/work/attendance/lrm_scripts/{lang}/{current_date.strftime("%d_%m_%y")}.csv'
            )
            os.remove(
                f'/home/sai/work/attendance/lrm_scripts/{lang}/{current_date.strftime("%d_%m_%y")}.csv'
            )

            tdf = tdf[
                [
                    "Name",
                    "LC Type",
                    "Daily duration (Sec)",
                    "Date of QA check",
                    "QA Done by",
                ]
            ]
            df = pd.concat([df, tdf], ignore_index=True)
            current_date = current_date + timedelta(days=1)

        sleep(10)

    if df.empty:
        continue

    df = df.dropna(subset=["Date of QA check"]).dropna(subset=["Name"])
    df = df.dropna(subset=["QA Done by"])

    df["Daily duration (Sec)"].fillna(0, inplace=True)
    df = df[~df["Daily duration (Sec)"].astype(str).str.contains(":", na=False)]

    df.to_csv(
        f"/home/sai/work/attendance/lrm_scripts/{lang}/{lang}_raw.csv", index=False
    )
    sleep(10)


# LRM REPORT - REMAINING LANGS
projs = {"ai4b": ai4b_langs, "lrl": list(langs.keys() - ai4b_langs)}
# projs = {'ai4b': ['Kashmiri']}
channels = {"ai4b": "C05B2DX2MFA", "lrl": "C05B0MAK2F6"}
print(yesterday)
for proj, proj_langs in projs.items():
    df = pd.DataFrame()
    print(proj_langs)
    for lang in proj_langs:
        tdf = pd.read_csv(
            f"/home/sai/work/attendance/lrm_scripts/{lang}/{lang}_raw.csv"
        )
        tdf["Language"] = lang
        tdf["QA Done by"] = tdf["QA Done by"].str.strip().str.capitalize()
        df = pd.concat([df, tdf], ignore_index=True)

    df = df[df["Date of QA check"] == yesterday]
    df = df[["Language", "QA Done by", "Daily duration (Sec)", "LC Type"]]
    annotator_df = df[df["LC Type"] == "Annotators"]

    # Group by Language and Name, and sum the Daily duration for annotation
    annotation_duration = (
        annotator_df.groupby(["Language", "QA Done by"])["Daily duration (Sec)"]
        .sum()
        .reset_index()
    )
    annotation_duration["Daily duration (mins)"] = round(
        ((annotation_duration["Daily duration (Sec)"] / 60) / 60), 3
    )
    annotation_duration = annotation_duration[
        ["Language", "QA Done by", "Daily duration (mins)"]
    ]
    # Filter the DataFrame for reviewers
    reviewer_df = df[df["LC Type"] == "Reviewers"]

    # Group by Language and Name, and sum the Daily duration for review
    review_duration = (
        reviewer_df.groupby(["Language", "QA Done by"])["Daily duration (Sec)"]
        .sum()
        .reset_index()
    )
    review_duration["Daily duration (mins)"] = round(
        ((review_duration["Daily duration (Sec)"] / 60) / 60), 3
    )
    review_duration = review_duration[
        ["Language", "QA Done by", "Daily duration (mins)"]
    ]

    # Merge the two DataFrames on Language and Name
    result_df = pd.merge(
        annotation_duration,
        review_duration,
        on=["Language", "QA Done by"],
        suffixes=("_annotation", "_review"),
        how="outer",
    )
    # grouped_df['Daily duration (mins)'] = round((grouped_df['Daily duration (Sec)'] / 60), 3)

    result_df = result_df.rename(
        columns={
            "Daily duration (mins)_annotation": "Annotation duration QA",
            "Daily duration (mins)_review": "Review durations QA",
        }
    )
    result_df = result_df[
        ["Language", "QA Done by", "Annotation duration QA", "Review durations QA"]
    ]
    result_df.to_csv(
        f"/home/sai/work/attendance/lrm_scripts/{proj}_LRM_REPORT.csv", index=False
    )

    try:
        response = slack_client.files_upload(
            channels=channels[proj],
            file=f"/home/sai/work/attendance/lrm_scripts/{proj}_LRM_REPORT.csv",
        )
        print(f"lrm_scripts/{proj}_LRM_REPORT.csv uploaded successfully!")

    except SlackApiError as e:
        error_message = e.response["error"]
        print(f"Error uploading file: {error_message}")
