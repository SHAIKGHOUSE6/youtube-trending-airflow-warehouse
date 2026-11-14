import os
import csv
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build

# Load environment variables from .env file
load_dotenv()
api_key = os.getenv("api_key")

youtube = build('youtube', 'v3', developerKey=api_key)

countries = {
    'India': 'IN',
    'US': 'US',
    'UK': 'GB',
    'Canada': 'CA',
    'Australia': 'AU',
    'Germany': 'DE',
    'France': 'FR',
    'Brazil': 'BR',
    'Japan': 'JP',
    'South Korea': 'KR',
    'Mexico': 'MX',
    'Pakistan': 'PK'
}

all_data = []

today = datetime.now()
date = today.strftime("%Y-%m-%d")
year = today.year
week_number = today.isocalendar().week

for country_name, country_code in countries.items():
    try:
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            chart='mostPopular',
            regionCode=country_code,
            maxResults=50
        )
        response = request.execute()

        for item in response.get('items', []):
            views = int(item['statistics'].get('viewCount', 0))
            likes = int(item['statistics'].get('likeCount', 0))
            comments = int(item['statistics'].get('commentCount', 0))

            video_id = item["id"]

            title = item["snippet"]["title"].replace("\n", " ").replace("\r", " ")
            channel_id = item["snippet"]["channelId"]
            channel_name = item["snippet"]["channelTitle"].replace("\n", " ").replace("\r", " ")

            all_data.append({
                'Date': date,
                'Year': year,
                'Week': week_number,
                'Country_Code': country_code,
                'Country': country_name,
                'Video_ID': video_id,
                'Title': title,
                'Channel_ID': channel_id,
                'Channel': channel_name,
                'PublishedAt': item['snippet']['publishedAt'],
                'Views': views,
                'Likes': likes,
                'Comments': comments,
                'Duration': item['contentDetails'].get('duration'),
                'Definition': item['contentDetails'].get('definition'),
                'Caption_Available': item['contentDetails'].get('caption')
            })

        print(f"Fetched Successfully: {country_name}")

    except Exception as e:
        print(f"Error in {country_name}: {e}")

new_df = pd.DataFrame(all_data)
print(new_df.columns.tolist())

print("\nNew data extracted:")
print(new_df.shape)
print(new_df.head())

new_df = new_df.drop_duplicates()

# Ensure the csv_files directory exists and point to the master CSV inside it
folder_name = "csv_files"
os.makedirs(folder_name, exist_ok=True)
file_path = os.path.join(folder_name, "youtube_trending_data.csv")

if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
    existing_df = pd.read_csv(file_path, encoding="utf-8")
    print("\nExisting master data:")
    print(existing_df.shape)
else:
    existing_df = pd.DataFrame()
    print("\nNo existing master CSV found.")

master_df = pd.concat([existing_df, new_df], ignore_index=True)

master_df = master_df.drop_duplicates(
    subset=["Video_ID", "Country_Code", "Date"],
    keep="last"
)

master_df.to_csv(
    file_path,
    index=False,
    encoding="utf-8",
    quoting=csv.QUOTE_ALL
)

print("\nETL Extraction Completed Successfully!")
print("Master CSV shape:", master_df.shape)
print("\nMaster CSV columns:")
print(master_df.columns.tolist())
print("\nLatest data:")
print(master_df.tail())