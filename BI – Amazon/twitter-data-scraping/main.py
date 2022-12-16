import os
import time
import requests
import asyncio
from datetime import datetime
from prisma import Prisma

# https://developer.twitter.com/en/docs/twitter-api/tweets/search/introduction
# https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query
# https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
# https://developer.twitter.com/apitools/api?endpoint=%2F2%2Ftweets%2Fsearch%2Frecent&method=get
# https://developer.twitter.com/en/docs/twitter-api/annotations/overview
# https://prisma-client-py.readthedocs.io/en/stable/

BEARER_TOKEN = os.environ['BEARER_TOKEN']
TWITTER_SEARCH_BASE_URL = "https://api.twitter.com/2/tweets/search/all"

def getNextTweetPage(
    day,
    page_token = ""
):

    # Filter for tweets which have been classified as English
    language = "lang%3Aen%20"

    # Context: "Brand" 47
    # Entity: "Amazon" 10026792024
    context = "context%3A47.10026792024%20"

    # Keywords: Amazon, Alexa, Superbowl, "Scarlett Johansson", "Colin Jost", "Amazon Superbowl ad", "Amazon Superbowl commercial"
    keywords = "(Amazon%20OR%20Alexa%20OR%20Superbowl%20OR%20%22Scarlett%20Johansson%22%20OR%20%22Colin%20Jost%22%20OR%20%22Superbowl%20Ad%22%20OR%20%22Superbowl%20commercial%22)"

    # Start time: February 8th 12:00am UTC
    # End time: Februrary 28th 11:59pm UTC
    date_range = f"&start_time=2022-02-{day}T00:00:00.000Z&end_time=2022-02-{day}T23:59:00.000Z"

    # How many results to include per request (max. 500)
    page_size = "&max_results=500"

    # Fields to include:
    # - Content of tweet (text)
    # - Geographical data, if it exists
    # - Public metrics (Retweet count, Quote count, Liked count, Reply count)
    fields = "&tweet.fields=created_at,geo,public_metrics"

    expansions = "&expansions=geo.place_id&place.fields=country"

    # Page token, if it exists
    next_token = f"&next_token={page_token}" if page_token else ""

    r = requests.get(
        f"{TWITTER_SEARCH_BASE_URL}?query={keywords}{language}{context}{date_range}{page_size}{fields}{expansions}{next_token}",
        headers={
            "Authorization" : f"Bearer {BEARER_TOKEN}"
        }
    )

    json = r.json()

    meta = {}
    try:
        meta = json['meta']
    except: 
        return ("Rate-limited", json, {}) 

    next_token = page_token 

    try:
        next_token = meta['next_token']
    except: None 

    places = {}
    try: 
        includes = json['includes']
        for place in includes['places']:
            id = place['id']
            places[id] = {
                'id' : id,
                'locality' : place['full_name'],
                'country' : place['country']
            }
    except: None


    data = json['data'] 

    tweets = {}
    for tweet in data:
        id = tweet['id']
        text = tweet['text']
        created_at = tweet['created_at']
        metrics = tweet['public_metrics']
        retweet_count = metrics['retweet_count']
        reply_count = metrics['reply_count']
        like_count = metrics['like_count']
        quote_count = metrics['quote_count']
        place_id = None

        try:
            geo = tweet['geo']
            place_id = geo['place_id']
        except: None 

        tweets[id] = {
            'id' : id,
            'text' : text,
            'created_at' : created_at,
            'retweet_count' : retweet_count if retweet_count else 0,
            'reply_count' : reply_count if reply_count else 0,
            'like_count' : like_count if like_count else 0,
            'quote_count' : quote_count if quote_count else 0,
            'place_id' : place_id
        }

    return (next_token, tweets, places)

async def insertTweets(prisma, day):
    places = {}
    tweets = {}
    page_token = ""
    tweets_count = 0

    for page in range(0, 100): # Rate-limit: 300 requests per 15 minutes
        (next_token, _tweets, _places) = getNextTweetPage(day, page_token)

        if next_token == "Rate-limited": return (False, _tweets)

        count = len(_tweets)
        tweets_count += count
        print(f"Got {count} tweets and {len(_places)} places for page {page} with token {page_token} for day {day}")
        if page_token == next_token:
            print(f"Exited early, end of results for day {day}")
            break 
        else:
            places.update(_places)
            tweets.update(_tweets)
            page_token = next_token

        time.sleep(1.01) # Rate-limit: 1 request per second

    await prisma.place.create_many(
        data = [ places[id] for id in places ],
        skip_duplicates = True
    )

    await prisma.tweet.create_many(
        data = [ tweets[id] for id in tweets ],
        skip_duplicates = True
    )

    print(f"End of results for day {day}. Inserted {tweets_count} tweets. Final page token = {page_token}.")

    return (True, {})


async def main() -> None:
    prisma = Prisma()
    await prisma.connect()

    now = datetime.now()
    time = now.strftime("%H:%M:%S")
    print(f"Starting script at: {time}...")

    for day in range(27, 29):
        (success, response) = await insertTweets(prisma, f"{day}".rjust(2, "0"))
        if success == False:
            then = datetime.now() - now
            print(response)
            print(f"Rate-limited. Total execution time: {then.seconds / 60} minutes")
            break

    await prisma.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
