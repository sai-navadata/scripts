# %%
import csv
import os
from datetime import datetime, timedelta
from time import sleep

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from requests import post
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tqdm import tqdm

slack_api_token = os.environ["SLACK_API_TOKEN"]
slack_client = WebClient(token=slack_api_token)

languages = {
    "Bengali": "https://docs.google.com/spreadsheets/d/1VnM6uCL6nkkGBsYZ49z5a7JCJ3EPMf83P7LQs8NCQMc/edit#gid=728021100",
    "Odia": "https://docs.google.com/spreadsheets/d/1w6RBlyvFfnoJut7fYH_VlQUATiwKDY6DchZE0nMIDKQ/edit#gid=547042898",
    "Assamese": "https://docs.google.com/spreadsheets/d/17wpO0wZZt5frjkY3-J7KZ5IotW9BQKLpeJuoPTdxMs0/edit#gid=682966645",
    "Bodo": "https://docs.google.com/spreadsheets/d/1uSL7hlJZNbigam4FZcS8C5fYIDYJvgjJTJ6eatx-f3A/edit#gid=161684224",
    "Konkani": "https://docs.google.com/spreadsheets/d/1n7ouVdOnPKGXlT_5zz8KJGecSxqTET08QdmM_Z42IDY/edit#gid=37977472",
    "Kashmiri": "https://docs.google.com/spreadsheets/d/1kxrsoWipfCF85ddp5-ztjxB6dlG92vTR0TA-eUO59Qs/edit#gid=614108795",
    "Maithili": "https://docs.google.com/spreadsheets/d/1ttBnuf5CjWKOkB_hwWlAEP-jqLLPZegFGrGhD9-ZrnE/edit#gid=196609157",
    "Sanskrit": "https://docs.google.com/spreadsheets/d/130B1y0zjjihSoVAqKfPNvbvQFOUrHfDvLiZjB-PrIas/edit#gid=1173332346",
}

channels = {
    "Bengali": "C05KHDUPBFA",
    "Odia": "C05KBCZ8EAJ",
    "Assamese": "C05LZQGMFCZ",
    "Bodo": "C05M3STAZJ9",
    "Konkani": "C05KXT98GQ6",
    "Kashmiri": "C05PNG9ELEP",
    "Maithili": "C05M98L22VA",
    "Sanskrit": "C05LM80KHCJ",
}

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

result_sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1qmLkF_TXURbOe6-G5Iu48YtizzyCutSvVoUAVI4yN00/edit#gid=0"
)


current_date = datetime.now()
yesterday = current_date - timedelta(days=1)
if yesterday.weekday() == 6:  # Sunday corresponds to 6 in the weekday() function
    yesterday = yesterday - timedelta(days=1)  # Subtract one more day for Saturday

yesterday = yesterday.strftime("%d/%m/%y")

for lang, url in tqdm(languages.items()):
    sleep(20)
    try:
        sheet = client.open_by_url(url)
        worksheet = sheet.worksheet(yesterday)

        data = worksheet.get_values()

        with open(f"output/{lang}_LC_REPORT_RAW.csv", "w+", newline="") as file:
            writer = csv.writer(file)
            for row in data:
                writer.writerow(row)

        df = pd.read_csv(f"output/{lang}_LC_REPORT_RAW.csv")
        df = df[["Name", "LC Type", "Daily duration (Sec)"]].dropna(how="all")
        grouped_df = (
            df.groupby(["Name", "LC Type"])["Daily duration (Sec)"].sum().reset_index()
        )

        grouped_df["Daily Duration (mins)"] = round(
            (grouped_df["Daily duration (Sec)"] / 60), 3
        )
        grouped_df = (
            grouped_df[["Name", "LC Type", "Daily Duration (mins)"]]
            .sort_values("Daily Duration (mins)", ascending=False)
            .reset_index()
        )

        grouped_df.to_csv(f"output/{lang}_LC_REPORT.csv", index=False)
        try:
            response = slack_client.files_upload(
                channels=channels[lang], file=f"output/{lang}_LC_REPORT.csv"
            )
            print(f"File output/{lang}_LC_REPORT.csv uploaded successfully!")

        except SlackApiError as e:
            error_message = e.response["error"]
            print(f"Error uploading file: {error_message}")

    except Exception as e:
        print(e, lang)
    finally:
        continue
