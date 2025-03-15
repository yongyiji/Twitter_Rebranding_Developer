import os
import requests
import json

os.environ["BEARER_TOKEN"] = "" # Replace with your BEARER_TOKEN

bearer_token = os.getenv("BEARER_TOKEN")
# Check if token is set
if not bearer_token:
    print("Bearer token is NOT set.")
else:
    print("Bearer token is set.")

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2TweetLookupPython"
    return r

def get_user_id(username):
    """
    Get the user ID for a given username.
    """
    url = f"https://api.twitter.com/2/users/by/username/{username}"
    response = requests.get(url, auth=bearer_oauth)
    if response.status_code != 200:
        raise Exception(
            f"Request returned an error: {response.status_code} {response.text}"
        )
    user_data = response.json()
    return user_data['data']['id']

def get_user_tweets(user_id, max_results=10):
    """
    Fetch recent tweets for a user by user ID.
    """
    url = f"https://api.twitter.com/2/users/{user_id}/tweets"
    params = {
        "tweet.fields": "lang,author_id,created_at,text,public_metrics",
        "max_results": max_results  # Maximum tweets to fetch (10 to 100)
    }
    response = requests.get(url, params=params, auth=bearer_oauth)
    if response.status_code != 200:
        raise Exception(
            f"Request returned an error: {response.status_code} {response.text}"
        )
    return response.json()


def main():
    username = "Username"  # Replace with the username of interest
    try:
        # Get the user ID
        user_id = get_user_id(username)
        print(f"User ID for {username}: {user_id}")

        # Fetch recent tweets for the user
        tweets = get_user_tweets(user_id, max_results=10)  # Adjust max_results as needed
        print(json.dumps(tweets, indent=4, sort_keys=True))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
