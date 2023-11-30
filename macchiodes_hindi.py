# %%
import csv
import os
from datetime import datetime, timedelta

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from requests import post

languages = {
    "Machiodes": "https://docs.google.com/spreadsheets/d/10gTPpCjG-Mr2Ao6r3HLuj7HnifkYH6KyA3Ktu56Fq9A/edit#gid=429049043"
}

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# result_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qmLkF_TXURbOe6-G5Iu48YtizzyCutSvVoUAVI4yN00/edit#gid=0")


current_date = datetime.now()
yesterday = current_date - timedelta(days=1)
if yesterday.weekday() == 6:  # Sunday corresponds to 6 in the weekday() function
    yesterday = yesterday - timedelta(days=1)  # Subtract one more day for Saturday

yesterday = yesterday.strftime("%d/%m/%y")

for lang, url in languages.items():
    try:
        sheet = client.open_by_url(url)
        worksheet = sheet.worksheet(yesterday)

        data = worksheet.get_all_records()

        with open(f"output/{lang}.csv", "w+", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                writer.writerow(row)

        df = pd.read_csv(f"output/{lang}.csv")
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
        url = os.environ["SLACK_MACH_HINDI_WEBHOOK"]
        headers = {"Content-type": "application/json"}
        data = {
            "text": f"<!channel> Summary - {yesterday}\n```{grouped_df.to_markdown(headers='keys', index=False)}```"
        }

        response = post(url, headers=headers, json=data)

        print(response.status_code)
        print(response.text)

    except Exception as e:
        print(e, lang)
    finally:
        continue
