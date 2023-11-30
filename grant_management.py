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
    "Bengali": "https://docs.google.com/spreadsheets/d/1VnM6uCL6nkkGBsYZ49z5a7JCJ3EPMf83P7LQs8NCQMc/edit#gid=479744458",
    "Odia": "https://docs.google.com/spreadsheets/d/1w6RBlyvFfnoJut7fYH_VlQUATiwKDY6DchZE0nMIDKQ/edit#gid=38491920",
    "Sanskrit": "https://docs.google.com/spreadsheets/d/130B1y0zjjihSoVAqKfPNvbvQFOUrHfDvLiZjB-PrIas/edit#gid=1771813459",
    "Kashmiri": "https://docs.google.com/spreadsheets/d/1kxrsoWipfCF85ddp5-ztjxB6dlG92vTR0TA-eUO59Qs/edit#gid=695335768",
    "Assamese": "https://docs.google.com/spreadsheets/d/1fEjJwISvUDUSTGW6LAgiyJTu2mMTiOSrgqc2fhIZ4l4/edit#gid=1989286070",
    "Konkani": "https://docs.google.com/spreadsheets/d/1n7ouVdOnPKGXlT_5zz8KJGecSxqTET08QdmM_Z42IDY/edit#gid=976292910",
    "Bodo": "https://docs.google.com/spreadsheets/d/1uSL7hlJZNbigam4FZcS8C5fYIDYJvgjJTJ6eatx-f3A/edit#gid=1201016662",
    "Dogri": "https://docs.google.com/spreadsheets/d/1e8sYCMgJl2jGnhXSSFC7dkqfswCBlAo1SkECss4rEe0/edit#gid=1765100185",
    "Nepali": "https://docs.google.com/spreadsheets/d/1NVyw1q_D9yoNHI5QiT_dEwCerQAuV9bHq0V1MRJpA6w/edit#gid=197187240",
    "Maithili": "https://docs.google.com/spreadsheets/d/1ttBnuf5CjWKOkB_hwWlAEP-jqLLPZegFGrGhD9-ZrnE/edit#gid=1126384288",
}


# %%
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

result_sheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1bhSP33181nR2dnNRCTfOllbJ_FOjar4fhKs9yNkfUs0/edit#gid=0"
)


current_date = datetime.now()
yesterday = current_date - timedelta(days=1)
yesterday = yesterday.strftime("%d/%m/%y")

for lang, url in languages.items():
    try:
        sheet = client.open_by_url(url)
        worksheet = sheet.worksheet(yesterday)

        data = worksheet.get_values()

        with open(f"output/{lang}.csv", "w+", newline="") as file:
            writer = csv.writer(file)
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

        worksheet = result_sheet.worksheet(lang)
        worksheet.clear()
        set_with_dataframe(worksheet, grouped_df, include_index=False)
    except Exception as e:
        print(e, lang)
    finally:
        continue

url = os.environ["SLACK_GRANT_WEBHOOK"]
headers = {"Content-type": "application/json"}
data = {
    "text": "<!channel> Sheets ready\nURL: https://docs.google.com/spreadsheets/d/1bhSP33181nR2dnNRCTfOllbJ_FOjar4fhKs9yNkfUs0/edit#gid=0"
}

response = post(url, headers=headers, json=data)

print(response.status_code)
print(response.text)
