import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Configure instance list
instances = [
    'https://mastodon.social',
    'https://fosstodon.org'
]

access_token = 'your_access_token'
keyword_variants = ['github.com']
max_posts_per_instance = 500  # Collect 2 posts with keywords per instance to identify users

# Set request headers
headers = {'Authorization': f'Bearer {access_token}'}

# Store results
unique_users = set()
all_posts_data = []

# Fetch posts with keywords from each instance and identify users
for api_base_url in instances:
    posts_collected_for_instance = 0
    print(f"Searching instance to identify users: {api_base_url}")
    timeline_url = f'{api_base_url}/api/v1/timelines/public'
    params = {'limit': 40, 'local': False}

    has_more = True
    try:
        while has_more and posts_collected_for_instance < max_posts_per_instance:
            response = requests.get(timeline_url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                statuses = response.json()
                if not statuses:
                    print(f"{api_base_url} timeline returned empty data")
                    break
                for post in statuses:
                    is_reblog = 'reblog' in post and post['reblog'] is not None
                    content = post['reblog']['content'] if is_reblog else post['content']
                    plain_text = BeautifulSoup(content, 'html.parser').get_text().lower()
                    if any(kw.lower() in plain_text for kw in keyword_variants):
                        user_id = post['account']['id']
                        unique_users.add((api_base_url, user_id))
                        posts_collected_for_instance += 1
                        if posts_collected_for_instance >= max_posts_per_instance:
                            break
                if 'Link' in response.headers:
                    links = response.headers['Link'].split(', ')
                    for link in links:
                        if 'rel="next"' in link:
                            timeline_url = link.split(';')[0][1:-1]
                            break
                    else:
                        has_more = False
                else:
                    has_more = False
                print(f"{api_base_url} identified: {posts_collected_for_instance} posts with keywords (total users: {len(unique_users)})")
                time.sleep(2)
            else:
                print(f"{api_base_url} failed to fetch timeline, status code: {response.status_code}")
                break
    except requests.RequestException as e:
        print(f"{api_base_url} request failed: {e}")
        continue

print(f"Identified a total of {len(unique_users)} unique users from all instances")

# Fetch all posts from identified users
for instance, user_id in unique_users:
    api_base_url = instance

    # Fetch user information
    user_info_url = f'{api_base_url}/api/v1/accounts/{user_id}'
    try:
        user_info_response = requests.get(user_info_url, headers=headers, timeout=10)
        if user_info_response.status_code == 200:
            user_info = user_info_response.json()
            username = user_info['username']
            display_name = user_info['display_name']
            followers_count = user_info['followers_count']
            following_count = user_info['following_count']
        else:
            print(f"{instance} failed to fetch user info (ID: {user_id}), status code: {user_info_response.status_code}")
            continue
    except requests.RequestException as e:
        print(f"{instance} failed to fetch user info: {e}")
        continue

    # Fetch all posts from the user
    statuses_url = f'{api_base_url}/api/v1/accounts/{user_id}/statuses'
    params = {'limit': 40, 'exclude_replies': False, 'only_media': False}
    has_more = True
    page_count = 0
    total_posts = 0

    try:
        while has_more:
            response = requests.get(statuses_url, params=params, headers=headers, timeout=10)
            page_count += 1
            if response.status_code == 200:
                statuses = response.json()
                if not statuses:
                    print(f"{instance} user {user_id} page {page_count} returned empty data, all posts may have been fetched")
                    break
                for post in statuses:
                    is_reblog = 'reblog' in post and post['reblog'] is not None
                    content = post['reblog']['content'] if is_reblog else post['content']
                    plain_text = BeautifulSoup(content, 'html.parser').get_text()
                    all_posts_data.append({
                        'instance': instance,
                        'user_id': user_id,
                        'username': username,
                        'display_name': display_name,
                        'post_id': post['id'],
                        'content': plain_text,
                        'created_at': post['created_at'],
                        'favourites_count': post['favourites_count'] if not is_reblog else post['reblog']['favourites_count'],
                        'reblogs_count': post['reblogs_count'] if not is_reblog else post['reblog']['reblogs_count'],
                        'replies_count': post['replies_count'] if not is_reblog else post['reblog']['replies_count'],
                        'is_reblog': is_reblog,
                        'followers_count': followers_count,
                        'following_count': following_count
                    })
                    total_posts += 1

                # Check for pagination
                if 'Link' in response.headers:
                    links = response.headers['Link'].split(', ')
                    next_url = None
                    for link in links:
                        if 'rel="next"' in link:
                            next_url = link.split(';')[0][1:-1]
                            break
                    if next_url:
                        statuses_url = next_url
                    else:
                        has_more = False
                        print(f"{instance} user {user_id} has no more pages, fetched {total_posts} posts")
                else:
                    has_more = False
                    print(f"{instance} user {user_id} response headers lack Link, fetched {total_posts} posts")
                time.sleep(2)  # Avoid rate limiting
            elif response.status_code == 429:
                print(f"{instance} user {user_id} hit rate limit, retrying after 5 minutes")
                time.sleep(300)  # Wait 5 minutes
                continue
            else:
                print(f"{instance} failed to fetch posts for user {user_id}, page {page_count}, status code: {response.status_code}")
                break
        print(f"{instance} user {user_id} fetched a total of {total_posts} posts ({page_count} pages)")
    except requests.RequestException as e:
        print(f"{instance} failed to fetch posts for user {user_id}: {e}")

# Create DataFrame
df = pd.DataFrame(all_posts_data)

# Ensure correct column order
desired_columns = [
    'instance', 'user_id', 'username', 'display_name', 'post_id', 'content',
    'created_at', 'favourites_count', 'reblogs_count', 'replies_count',
    'is_reblog', 'followers_count', 'following_count'
]
df = df[desired_columns]

# Output results
print("\n### Final DataFrame (one post per row, including user info and whether it’s a reblog) ###")
print(df.head())
print(f"Collected a total of {len(df)} posts")

# Save to CSV file
output_path = 'your_file_location'
df.to_csv(output_path, index=False)
print(f"Data saved to {output_path}")
