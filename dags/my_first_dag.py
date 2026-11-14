from datetime import datetime, timedelta
import os
import re
import csv
import pandas as pd
from googleapiclient.discovery import build

from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Directory where all CSVs will be stored (ensure ./csv_files:/opt/airflow/csv_files is mounted in Docker)
DATA_DIR = "/opt/airflow/csv_files"
MASTER_CSV_PATH = os.path.join(DATA_DIR, "youtube_trending_data.csv")

COUNTRIES = {
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

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='youtube_trending_etl',
    default_args=default_args,
    description='Extracts YouTube trending videos, builds star-schema dimensions/facts, and loads to Postgres',
    schedule='@daily',  # Runs once a day at midnight UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['youtube', 'etl', 'postgres']
)
def youtube_trending_pipeline():

    @task
    def extract_youtube_data() -> str:
        """Fetches trending videos from YouTube API and appends to master CSV."""
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Fetch API key securely from Airflow Variables
        api_key = Variable.get("YOUTUBE_API_KEY")
        youtube = build('youtube', 'v3', developerKey=api_key)

        all_data = []
        today = datetime.now()
        date_str = today.strftime("%Y-%m-%d")
        year = today.year
        week_number = today.isocalendar().week

        for country_name, country_code in COUNTRIES.items():
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

                    title = item["snippet"]["title"].replace("\n", " ").replace("\r", " ")
                    channel_name = item["snippet"]["channelTitle"].replace("\n", " ").replace("\r", " ")

                    all_data.append({
                        'Date': date_str,
                        'Year': year,
                        'Week': week_number,
                        'Country_Code': country_code,
                        'Country': country_name,
                        'Video_ID': item["id"],
                        'Title': title,
                        'Channel_ID': item["snippet"]["channelId"],
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

        new_df = pd.DataFrame(all_data).drop_duplicates()

        if os.path.exists(MASTER_CSV_PATH) and os.path.getsize(MASTER_CSV_PATH) > 0:
            existing_df = pd.read_csv(MASTER_CSV_PATH, encoding="utf-8")
        else:
            existing_df = pd.DataFrame()

        master_df = pd.concat([existing_df, new_df], ignore_index=True)
        master_df = master_df.drop_duplicates(
            subset=["Video_ID", "Country_Code", "Date"],
            keep="last"
        )

        master_df.to_csv(
            MASTER_CSV_PATH,
            index=False,
            encoding="utf-8",
            quoting=csv.QUOTE_ALL
        )
        print(f"Extraction complete. Master CSV shape: {master_df.shape}")
        return MASTER_CSV_PATH

    @task
    def transform_star_schema(master_csv_path: str) -> dict:
        """Cleans master data and splits it into Dimension and Fact CSVs."""
        df = pd.read_csv(master_csv_path)

        # 1. Deduplicate
        df = df.drop_duplicates(subset=['Video_ID', 'Channel_ID', 'Date'], keep='first')

        # 2. Parse timestamps
        df['PublishedAt'] = pd.to_datetime(df['PublishedAt'], utc=True)
        df['Published_Date'] = df['PublishedAt'].dt.date
        df['Published_Month'] = df['PublishedAt'].dt.month
        df['Published_Day_Name'] = df['PublishedAt'].dt.day_name()

        # 3. Parse ISO 8601 Duration to seconds
        def duration_to_seconds(duration):
            if pd.isna(duration):
                return 0
            hours = re.search(r'(\d+)H', str(duration))
            minutes = re.search(r'(\d+)M', str(duration))
            seconds = re.search(r'(\d+)S', str(duration))

            h = int(hours.group(1)) if hours else 0
            m = int(minutes.group(1)) if minutes else 0
            s = int(seconds.group(1)) if seconds else 0
            return h * 3600 + m * 60 + s

        df['Duration_Seconds'] = df['Duration'].apply(duration_to_seconds)

        # 4. Drop raw columns & lowercase all column names
        mega_df = df.drop(columns=['Duration', 'PublishedAt'], errors='ignore')
        mega_df.columns = mega_df.columns.str.lower()

        # 5. Build Dimension Tables
        dim_channel = (
            mega_df[['channel_id', 'channel']]
            .drop_duplicates(subset=['channel_id'], keep='first')
            .reset_index(drop=True)
        )

        dim_video = (
            mega_df[[
                'video_id', 'channel_id', 'title', 'published_date',
                'duration_seconds', 'definition', 'caption_available', 'country_code'
            ]]
            .drop_duplicates(subset=['video_id'], keep='first')
            .reset_index(drop=True)
        )

        dim_country = (
            mega_df[['country_code', 'country']]
            .drop_duplicates(subset=['country_code'], keep='first')
            .reset_index(drop=True)
        )
        dim_country.insert(0, 'country_id', range(1, len(dim_country) + 1))

        mega_df['date'] = pd.to_datetime(mega_df['date'])
        mega_df['published_date'] = pd.to_datetime(mega_df['published_date'])

        dim_date = (
            mega_df[['date', 'year', 'week', 'published_date']]
            .drop_duplicates(subset=['date'], keep='first')
            .reset_index(drop=True)
        )
        dim_date['month'] = dim_date['published_date'].dt.month
        dim_date['published_day'] = dim_date['published_date'].dt.day
        dim_date['published_day_name'] = dim_date['published_date'].dt.day_name()
        dim_date.insert(0, 'date_id', range(1, len(dim_date) + 1))

        # 6. Build Fact Table
        fact_trending = (
            mega_df[['date', 'video_id', 'views', 'likes', 'comments', 'country_code','duration_seconds']]
            .drop_duplicates(subset=['video_id', 'country_code', 'date'], keep='first')
            .reset_index(drop=True)
        )

        # 7. Save processed tables to csv_files directory and return file paths via XCom
        tables = {
            'dim_video': dim_video,
            'dim_channel': dim_channel,
            'dim_country': dim_country,
            'dim_date': dim_date,
            'fact_trending': fact_trending,
        }

        output_paths = {}
        for table_name, table_df in tables.items():
            file_path = os.path.join(DATA_DIR, f"{table_name}.csv")
            table_df.to_csv(file_path, index=False)
            output_paths[table_name] = file_path
            print(f"Saved {table_name}: {table_df.shape}")

        return output_paths

    @task
    def load_to_postgres(table_paths: dict):
        """Loads all transformed CSVs into PostgreSQL."""
        # Uses Airflow Connection ID 'postgres_youtube'
        pg_hook = PostgresHook(postgres_conn_id='postgres_youtube')
        engine = pg_hook.get_sqlalchemy_engine()

        for table_name, file_path in table_paths.items():
            df = pd.read_csv(file_path)
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False
            )
            print(f"Successfully loaded {len(df)} rows into table '{table_name}'.")

    # Define Task Dependencies
    raw_file = extract_youtube_data()
    transformed_files = transform_star_schema(raw_file)
    load_to_postgres(transformed_files)


# Instantiate the DAG
youtube_trending_pipeline()