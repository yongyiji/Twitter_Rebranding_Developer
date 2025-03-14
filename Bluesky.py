import pandas as pd
from atproto import Client, models
from tqdm import tqdm
import time

# Initialize Bluesky client
at_client = Client()
USERNAME = "your_username"
PASSWD = "your_password"
at_client.login(USERNAME, PASSWD)

# Convert post object to dictionary and determine if it's a repost
def post_to_dict(post, is_repost=False):
    try:
        post_info = {
            'username': post.author.handle if hasattr(post.author, 'handle') else None,
            'post_uri': post.uri,
            'text': post.record.text if hasattr(post.record, 'text') else None,
            'created_at': post.record.created_at if hasattr(post.record, 'created_at') else None,
            'like_count': post.like_count if hasattr(post, 'like_count') else None,
            'reply_count': post.reply_count if hasattr(post, 'reply_count') else None,
            'repost_count': post.repost_count if hasattr(post, 'repost_count') else None,
            'lang': post.record.lang if hasattr(post.record, 'lang') else None,
            'embed_uri': (post.embed.external.uri
                          if hasattr(post, 'embed') and hasattr(post.embed, 'external')
                          and hasattr(post.embed.external, 'uri') else None),
            'is_repost': is_repost
        }
        return post_info
    except Exception as e:
        print(f"Error processing post: {e}")
        return {}

# Convert user profile to dictionary
def profile_to_dict(profile):
    try:
        return {
            'username': profile.handle,
            'did': profile.did,
            'display_name': profile.display_name,
            'followers_count': profile.followers_count,
            'follows_count': profile.follows_count,
            'posts_count': profile.posts_count,
            'description': profile.description
        }
    except Exception as e:
        print(f"Error processing profile: {e}")
        return {}

# Function to search posts by keyword
def search_posts_by_keyword(keyword, limit_per_request=50, max_posts=1000):
    post_data = []
    cursor = None

    while len(post_data) < max_posts:
        search_params = models.AppBskyFeedSearchPosts.Params(
            q=keyword,
            limit=limit_per_request,
            cursor=cursor
        )
        response = at_client.app.bsky.feed.search_posts(search_params)

        if not response.posts:
            print(f"No more posts found for keyword: {keyword}")
            break

        for post in response.posts:
            post_info = post_to_dict(post, is_repost=False)
            post_data.append(post_info)

        cursor = response.cursor
        if not cursor:
            print("Reached the end of search results.")
            break

        print(f"Collected {len(post_data)} posts so far...")
        time.sleep(1)

    return post_data[:max_posts]

# Get all posts of a user (no limit), determine if repost or original
def get_user_posts(username):
    post_data = []
    cursor = None

    try:
        params = models.AppBskyFeedGetAuthorFeed.Params(
            actor=username,
            limit=50
        )

        while True:
            if cursor:
                params.cursor = cursor

            response = at_client.app.bsky.feed.get_author_feed(params)

            if not response.feed:
                print(f"No more posts found for user: {username}")
                break

            for item in response.feed:
                is_repost = hasattr(item, 'reason') and isinstance(item.reason, models.AppBskyFeedDefs.ReasonRepost)
                post_info = post_to_dict(item.post, is_repost=is_repost)
                post_data.append(post_info)

            cursor = response.cursor
            if not cursor:
                print(f"Finished collecting all posts for {username}. Total: {len(post_data)} posts.")
                break

            print(f"Collected {len(post_data)} posts for {username} so far...")
            time.sleep(1)

    except Exception as e:
        print(f"Error fetching posts for {username}: {e}")

    return post_data

# Get user profile information
def get_user_profile(username):
    try:
        params = models.AppBskyActorGetProfile.Params(actor=username)
        profile = at_client.app.bsky.actor.get_profile(params)
        return profile_to_dict(profile)
    except Exception as e:
        print(f"Error fetching profile for {username}: {e}")
        return {}

# Main workflow
keyword = "github.com"
max_posts_to_collect = 1000  # Maximum number of posts to collect in the initial search

print(f"Step 1: Searching Bluesky for posts with keyword: {keyword}")
initial_posts = search_posts_by_keyword(keyword, limit_per_request=50, max_posts=max_posts_to_collect)
initial_posts_df = pd.DataFrame(initial_posts)

# Extract unique usernames
unique_usernames = initial_posts_df['username'].dropna().unique()
print(f"Found {len(unique_usernames)} unique users.")

# Collect all posts and profile information for each user
all_posts = []
user_profiles = []

for username in tqdm(unique_usernames, desc="Processing users"):
    user_posts = get_user_posts(username)
    all_posts.extend(user_posts)
    user_profile = get_user_profile(username)
    user_profiles.append(user_profile)

# Convert to DataFrame
all_posts_df = pd.DataFrame(all_posts)
user_profiles_df = pd.DataFrame(user_profiles)

# Merge DataFrames based on 'username'
merged_df = pd.merge(
    all_posts_df,
    user_profiles_df[['username', 'did', 'display_name', 'followers_count', 'follows_count', 'posts_count']],
    on='username',  # Merge only on 'username'
    how='left'
)

# Print results
print("\nUser Profiles:")
print(user_profiles_df[['username', 'display_name', 'followers_count', 'follows_count', 'posts_count']])
print("\nAll Posts:")
print(all_posts_df[['username', 'text', 'created_at', 'like_count', 'repost_count', 'reply_count', 'is_repost']])

# Save to CSV files
merged_df.to_csv("the location saves your data", index=False)
print("Data saved to 'bluesky_user_profiles.csv', 'bluesky_all_posts.csv', and 'merge_bluesky_all_posts.csv'")
