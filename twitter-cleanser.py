#!/usr/bin/env python

import tweepy
import json
import requests
import multiprocessing as mp
import argparse
import os

def read_config(filename):
    '''
    Reads twitter API access data from configuration file and returns parsed
    dictionary.
    '''
    try:
        with open(filename, 'r') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        print('Cannot find config file \'', filename, '\'')
    except json.JSONDecodeError as e:
        print('Cannot parse JSON-file: ', str(e))
    
    return None


def authenticate(config):
    '''
    Performs authentication to twitter API with provided config data
    and returns API-object which can then be used to access twitter feed.
    '''
    api = config['api']
    account = config['account']
    auth = tweepy.OAuthHandler(api['key'], api['secret'])
    auth.set_access_token(account['token'], account['secret'])
    try:
        api = tweepy.API(auth)
        return api
    except TweepError as e:
        print('Failed to authenticate: ', str(e))
    return None


def dump_to_file(tweet, file_name='tweets_dump.json'):
    '''
    Dumps tweet to 'file_name'. Each tweet is dumped on single line to make
    reading from this dump easier.
    '''
    try:
        with open(file_name, 'a') as f:
            f.write(tweet + '\n')
    except BaseException as e:
        print('Failed to dump tweet:', str(e))


def print_from_file(filename='tweets_dump.json'):
    '''
    Prints tweets read from 'file_name' to the screen.
    '''
    try:
        with open(filename, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                print(json.dumps(tweet, indent=2, ensure_ascii=False))
    except FileNotFoundError:
        print('Cannot find dump file \'', filename, '\'')
    except json.JSONDecodeError as e:
        print('Cannot parse JSON-file: ', str(e))


def load_tweets_from_file(filename):
    '''
    Loads tweets from 'filename' to python list and returns it.
    File should contain tweets in JSON format, each tweet in its own line.
    '''
    tweets = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                tweets.append(json.loads(line))
    except FileNotFoundError:
        print('Cannot find dump file \'', filename, '\'')
    except json.JSONDecodeError as e:
        print('Cannot parse JSON-file: ', str(e))

    return tweets


def get_tweets(api, tweets_to_read=0, dump_file=None, echo=True):
    '''
    Reads 'tweets_to_read' number of tweets from user feed.
    Dumps tweets to 'dump_file' if one is specified.
    Prints tweets to screen if 'echo' == True and progress dots if not.
    '''
    if not echo:
        print('Reading tweets', end='', flush=True)
        
    n = 0
    for status in tweepy.Cursor(api.user_timeline).items(tweets_to_read):
        if dump_file:
            dump_to_file(json.dumps(status._json,
                                    separators=(',', ':'),
                                    ensure_ascii=False),
                         dump_file)
        if echo:
            print(status._json['created_at'] + ':\n' +
                    status._json['text'] + '\n')
        else:
            if not (n % 10):
                print('.', end='', flush=True)
        n += 1

    if not echo:
        print('done.')


def filter_retweets(tweets):
    return [tweet for tweet in tweets if not is_retweet(tweet)]


def filter_tweets_with_urls(tweets):
    '''
    Returns list of tweets containing urls in 'text'. This list is holds only
    'id', 'created_at', 'text' and 'urls' keys from original dict
    and additional key 'bad' to indicate if tweet contains any bad urls.
    '''
    tweets_with_urls = []
    for tweet in tweets:
        if contains_url(tweet):
            # some fancy dict comprehension with set intersections
            # here I assume that these keys are always present, so no error
            # checking for simplicity
            entry = { key: tweet[key]
                        for key in tweet.keys() & {'id', 'created_at', 'text'}}
            entry['urls'] = [url['expanded_url']
                                for url in tweet['entities']['urls']]
            entry['bad'] = False
            tweets_with_urls.append(entry)
    return tweets_with_urls


def check_urls_in_tweet(tweet, timeout=2.0):
    '''
    Checks all urls in tweet under 'urls' key and marks tweet to bad
    if any of the urls is dead, otherwise tweet is marked good.
    '''
    bad_status = (400, 401, 402, 404, 406, 410, 413, 500, 502, 504)
    for url in tweet['urls']:
        #print(tweet)
        try:
            r = requests.head(url, timeout=timeout)
            if r.status_code in bad_status:
                tweet['bad'] = True
        except requests.exceptions.Timeout:
            tweet['bad'] = True
        except requests.ConnectionError:
            tweet['bad'] = True
        if tweet['bad']:
            break #single dead url is enough to mark tweet bad
    return tweet


def is_retweet(tweet_json):
    '''
    Checks if tweet is a retweet, i.e. 'retweeted' == True or text starts
    with 'RT'
    '''
    if tweet_json['retweeted']:
        return True
    # I'd generally use regexp to parse unstructured data, but it seems that
    # all 'quoted retweets' just start with 'RT'
    if tweet_json['text'].startswith('RT'):
        return True

    return False


def contains_url(tweet_json):
    '''
    Checks if specified tweet text contains urls.
    Images appear in text as urls too, but we skip this case as these images
    are supposedly hosted by twitter.
    '''
    if tweet_json['entities']['urls']:
        return True
    return False


def delete_tweet(tweet, interactive=True):
    '''
    Delete tweet from user timeline.
    If 'interactive' == True, then user will be asked to confirm his actions.
    '''
    if interactive:
        if not request_confirmation('Delete \'' + tweet['text']):
            print('Skipping this tweet')
            return

    print('Deleting tweet')
    api.destroy_status(tweet['id'])


def request_confirmation(question='ok'):
    '''
    Ask user a 'question', read yes/no reply and return True/False respectively
    '''
    reply = input(question + ' [yn]? ')
    if reply.lower() in ('y','yes'):
        return True
    return False


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--configfile', help='configuration file', default='config.json')
    argparser.add_argument('--backupfile', help='backup file to dump tweets', default='tweet_dump.json')
    argparser.add_argument('--processes', help='number of parallel requests', default=4)
    args = argparser.parse_args()

    if os.path.exists(args.backupfile):
        if request_confirmation('Backup file exists, delete it'):
            os.remove(args.backupfile)


    config = read_config(args.configfile)
    api = authenticate(config)
    # it is always good to have a backup
    get_tweets(api, echo=False, dump_file=args.backupfile)

    # process tweet db
    tweets = load_tweets_from_file(args.backupfile)

    # check for dead urls in parallel
    pool = mp.Pool(processes=args.processes)
    result = pool.imap_unordered(check_urls_in_tweet,
            filter_tweets_with_urls(filter_retweets(tweets)))

    # delete bad tweets
    bad_tweets = [tweet for tweet in result if tweet['bad']]
    for tweet in bad_tweets:
        delete_tweet(tweet)
