import requests
import json
import os

from datetime import date
from airflow.decorators import task
from airflow.models import Variable

maxResults = 50

@task
def get_playlist_id():
    API_KEY = Variable.get("API_KEY")
    CHANNEL_HANDLE = Variable.get('CHANNEL_HANDLE')          # ✅ use the key name you set in Airflow UI
    url = (
        "https://youtube.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle=@{CHANNEL_HANDLE}&key={API_KEY}"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if "items" not in data or not data["items"]:
        raise ValueError(f"No channel found for handle @{CHANNEL_HANDLE}: {data}")

    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


@task
def get_video_ids(playlist_id):
    API_KEY = Variable.get("API_KEY")          # ✅ fetch inside task, was missing before
    video_ids = []
    page_token = None
    base_url = (
        "https://youtube.googleapis.com/youtube/v3/playlistItems"
        f"?part=contentDetails&maxResults={maxResults}"
        f"&playlistId={playlist_id}&key={API_KEY}"
    )

    while True:
        url = base_url + (f"&pageToken={page_token}" if page_token else "")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids


# ✅ removed @task — plain helper function
def batch_list(video_id_lst, batch_size):
    for i in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[i: i + batch_size]


@task
def extract_video_data(video_ids):
    API_KEY = Variable.get("API_KEY")          # ✅ fetch inside task, was missing before
    extracted_data = []

    try:
        for batch in batch_list(video_ids, maxResults):  # ✅ now calls plain function
            video_ids_str = ",".join(batch)
            url = (
                "https://youtube.googleapis.com/youtube/v3/videos"
                f"?part=contentDetails&part=snippet&part=statistics"
                f"&id={video_ids_str}&key={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": item["id"],
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount"),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e


@task                                          # ✅ add this
def save_to_json(extracted_data):
    os.makedirs("./data", exist_ok=True)
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
    print(f"Saved {len(extracted_data)} videos to {file_path}")

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)