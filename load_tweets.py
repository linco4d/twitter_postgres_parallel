#!/usr/bin/python3

# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################


def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    and there is no way to get the original twitter data back from the data in postgres.

    The null character is extremely rarely used in real world text (approx. 1 in 1 billion tweets),
    and so this isn't too big of a deal.
    A more correct implementation, however, would be to *escape* the null characters rather than remove them.
    This isn't hard to do in python, but it is a bit of a pain to do with the JSON/COPY commands for the denormalized data.
    Since our goal is for the normalized/denormalized versions of the data to match exactly,
    we're not going to escape the strings for the normalized data.

    >>> remove_nulls('\x00')
    ''
    >>> remove_nulls('hello\x00 world')
    'hello world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','')


def get_id_urls(url, connection):
    '''
    Given a url, return the corresponding id in the urls table.
    If no row exists for the url, then one is inserted automatically.

    NOTE:
    This function cannot be tested with standard python testing tools because it interacts with the db.
    '''
    sql = sqlalchemy.sql.text('''
    insert into urls
        (url)
        values
        (:url)
    on conflict do nothing
    returning id_urls
    ;
    ''')
    res = connection.execute(sql, {'url': url}).first()

    # when no conflict occurs, then the query above inserts a new row in the url table and returns id_urls in res[0];
    # when a conflict occurs, then the query above does not insert or return anything;
    # we need to run a select statement to put the already existing id_urls into res[0]
    if res is None:
        sql = sqlalchemy.sql.text('''
        select id_urls
        from urls
        where
            url=:url
        ''')
        res = connection.execute(sql, {'url': url}).first()

    id_urls = res[0]
    return id_urls

def insert_tweet(connection, tweet):
    '''
    Insert the tweet into the database.

    Args:
        connection: a sqlalchemy connection to the postgresql db
        tweet: a dictionary representing the json tweet object

    NOTE:
    This function cannot be tested with standard python testing tools because it interacts with the db.
    '''

    # begin one transaction for the whole insert
    with connection.begin():

        # skip tweet if it's already inserted
        sql = sqlalchemy.sql.text('''
        SELECT id_tweets 
        FROM tweets
        WHERE id_tweets = :id_tweets
        ''')
        res = connection.execute(sql, {
            'id_tweets': tweet['id'],
        })
        if res.first() is not None:
            return

        ########################################
        # insert into the users table
        ########################################
        if tweet['user']['url'] is None:
            user_id_urls = None
        else:
            user_id_urls = get_id_urls(tweet['user']['url'], connection)

        user_withheld = tweet['user'].get('withheld_in_countries', None)

        # create/update the user
        sql = sqlalchemy.sql.text('''
            insert into users (
                id_users,
                created_at,
                updated_at,
                id_urls,
                friends_count,
                listed_count,
                favourites_count,
                statuses_count,
                protected,
                verified,
                screen_name,
                name,
                location,
                description,
                withheld_in_countries
            ) values (
                :id_users,
                :created_at,
                now(),
                :id_urls,
                :friends_count,
                :listed_count,
                :favourites_count,
                :statuses_count,
                :protected,
                :verified,
                :screen_name,
                :name,
                :location,
                :description,
                :withheld_in_countries
            )
            on conflict (id_users) do update set
                created_at             = excluded.created_at,
                updated_at             = now(),
                id_urls                = excluded.id_urls,
                friends_count          = excluded.friends_count,
                listed_count           = excluded.listed_count,
                favourites_count       = excluded.favourites_count,
                statuses_count         = excluded.statuses_count,
                protected              = excluded.protected,
                verified               = excluded.verified,
                screen_name            = excluded.screen_name,
                name                   = excluded.name,
                location               = excluded.location,
                description            = excluded.description,
                withheld_in_countries  = excluded.withheld_in_countries
            ;
        ''')
        connection.execute(sql, {
            'id_users': tweet['user']['id'],
            'created_at': tweet['user'].get('created_at', None),
            'id_urls': user_id_urls,
            'friends_count': tweet['user'].get('friends_count', None),
            'listed_count': tweet['user'].get('listed_count', None),
            'favourites_count': tweet['user'].get('favourites_count', None),
            'statuses_count': tweet['user'].get('statuses_count', None),
            'protected': tweet['user'].get('protected', None),
            'verified': tweet['user'].get('verified', None),
            'screen_name': remove_nulls(tweet['user'].get('screen_name', None)),
            'name': remove_nulls(tweet['user'].get('name', None)),
            'location': remove_nulls(tweet['user'].get('location', None)),
            'description': remove_nulls(tweet['user'].get('description', None)),
            'withheld_in_countries': user_withheld,
        })

        ########################################
        # insert into the tweets table
        ########################################
        geo_str = None
        geo_coords = None

        try:
            # Twitter geo.coordinates is [lat, lon], but WKT expects "x y" = "lon lat"
            lat = tweet['geo']['coordinates'][0]
            lon = tweet['geo']['coordinates'][1]
            geo_coords = f'({lon} {lat})'
            geo_str = 'POINT'
        except (TypeError, KeyError):
            try:
                # Twitter place bounding_box.coordinates is a list of polygons,
                # each polygon is a list of [lon, lat] points.
                polygons = tweet['place']['bounding_box']['coordinates']
                poly_texts = []

                for poly in polygons:
                    ring_points = [f'{point[0]} {point[1]}' for point in poly]

                    # close the ring if necessary
                    if poly[0] != poly[-1]:
                        ring_points.append(f'{poly[0][0]} {poly[0][1]}')

                    # one polygon with one outer ring => ((...))
                    poly_texts.append(f"(({','.join(ring_points)}))")

                # multipolygon => MULTIPOLYGON( ((...)), ((...)) )
                geo_coords = f"({','.join(poly_texts)})"
                geo_str = 'MULTIPOLYGON'
            except (TypeError, KeyError):
                geo_str = None
                geo_coords = None

        try:
            text = tweet['extended_tweet']['full_text']
        except KeyError:
            text = tweet['text']

        try:
            country_code = tweet['place']['country_code'].lower()
        except (TypeError, KeyError):
            country_code = None

        if country_code == 'us':
            state_code = tweet['place']['full_name'].split(',')[-1].strip().lower()
            if len(state_code) > 2:
                state_code = None
        else:
            state_code = None

        try:
            place_name = tweet['place']['full_name']
        except (TypeError, KeyError):
            place_name = None

        tweet_withheld = tweet.get('withheld_in_countries', None)

        if tweet.get('in_reply_to_user_id', None) is not None:
            sql = sqlalchemy.sql.text('''
                insert into users (
                    id_users,
                    screen_name,
                    updated_at
                ) values (
                    :id_users,
                    :screen_name,
                    now()
                )
                on conflict do nothing
                ;
            ''')
            connection.execute(sql, {
                'id_users': tweet['in_reply_to_user_id'],
                'screen_name': remove_nulls(tweet.get('in_reply_to_screen_name', None)),
            })

        sql = sqlalchemy.sql.text('''
            insert into tweets (
                id_tweets,
                id_users,
                created_at,
                in_reply_to_status_id,
                in_reply_to_user_id,
                quoted_status_id,
                retweet_count,
                favorite_count,
                quote_count,
                withheld_copyright,
                withheld_in_countries,
                source,
                text,
                country_code,
                state_code,
                lang,
                place_name,
                geo
            ) values (
                :id_tweets,
                :id_users,
                :created_at,
                :in_reply_to_status_id,
                :in_reply_to_user_id,
                :quoted_status_id,
                :retweet_count,
                :favorite_count,
                :quote_count,
                :withheld_copyright,
                :withheld_in_countries,
                :source,
                :text,
                :country_code,
                :state_code,
                :lang,
                :place_name,
                ST_GeomFromText(:geo_wkt, 4326)
            )
            ;
        ''')
        connection.execute(sql, {
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet.get('created_at', None),
            'in_reply_to_status_id': tweet.get('in_reply_to_status_id', None),
            'in_reply_to_user_id': tweet.get('in_reply_to_user_id', None),
            'quoted_status_id': tweet.get('quoted_status_id', None),
            'retweet_count': tweet.get('retweet_count', None),
            'favorite_count': tweet.get('favorite_count', None),
            'quote_count': tweet.get('quote_count', None),
            'withheld_copyright': tweet.get('withheld_copyright', None),
            'withheld_in_countries': tweet_withheld,
            'source': remove_nulls(tweet.get('source', None)),
            'text': remove_nulls(text),
            'country_code': country_code,
            'state_code': state_code,
            'lang': tweet.get('lang', None),
            'place_name': remove_nulls(place_name),
            'geo_wkt': None if geo_str is None else f'{geo_str}{geo_coords}',
        })

        ########################################
        # insert into the tweet_urls table
        ########################################

        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']

        for url in urls:
            expanded_url = url.get('expanded_url', None)
            if expanded_url is None:
                continue

            id_urls = get_id_urls(expanded_url, connection)

            sql = sqlalchemy.sql.text('''
                insert into tweet_urls (
                    id_tweets,
                    id_urls
                ) values (
                    :id_tweets,
                    :id_urls
                )
                on conflict do nothing
                ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_urls': id_urls,
            })

        ########################################
        # insert into the tweet_mentions table
        ########################################

        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            sql = sqlalchemy.sql.text('''
                insert into users (
                    id_users,
                    screen_name,
                    updated_at
                ) values (
                    :id_users,
                    :screen_name,
                    now()
                )
                on conflict do nothing
                ;
            ''')
            connection.execute(sql, {
                'id_users': mention['id'],
                'screen_name': remove_nulls(mention.get('screen_name', None)),
            })

            sql = sqlalchemy.sql.text('''
                insert into tweet_mentions (
                    id_tweets,
                    id_users
                ) values (
                    :id_tweets,
                    :id_users
                )
                on conflict do nothing
                ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_users': mention['id'],
            })

        ########################################
        # insert into the tweet_tags table
        ########################################

        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags']
            cashtags = tweet['extended_tweet']['entities']['symbols']
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = ['#' + hashtag['text'] for hashtag in hashtags] + ['$' + cashtag['text'] for cashtag in cashtags]

        for tag in tags:
            sql = sqlalchemy.sql.text('''
                insert into tweet_tags (
                    id_tweets,
                    tag
                ) values (
                    :id_tweets,
                    :tag
                )
                on conflict do nothing
                ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'tag': remove_nulls(tag),
            })

        ########################################
        # insert into the tweet_media table
        ########################################

        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []

        for medium in media:
            media_url = medium.get('media_url', None)
            if media_url is None:
                continue

            id_urls = get_id_urls(media_url, connection)
            sql = sqlalchemy.sql.text('''
                insert into tweet_media (
                    id_tweets,
                    id_urls,
                    type
                ) values (
                    :id_tweets,
                    :id_urls,
                    :type
                )
                on conflict do nothing
                ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_urls': id_urls,
                'type': medium.get('type', None),
            })


################################################################################
# main functions
################################################################################

if __name__ == '__main__':

    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--print_every', type=int, default=1000)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets.py',
    })
    connection = engine.connect()

    # loop through the input file
    # NOTE:
    # we reverse sort the filenames because this results in fewer updates to the users table,
    # which prevents excessive dead tuples and autovacuums
    for filename in sorted(args.inputs, reverse=True):
        with zipfile.ZipFile(filename, 'r') as archive:
            print(datetime.datetime.now(), filename)
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for i, line in enumerate(f):

                        # load and insert the tweet
                        tweet = json.loads(line)
                        insert_tweet(connection, tweet)

                        # print message
                        if i % args.print_every == 0:
                            print(datetime.datetime.now(), filename, subfilename, 'i=', i, 'id=', tweet['id'])
