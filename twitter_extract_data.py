# import sentimentmodule as SM
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
# import MySQLdb
import json
import re
import string
import time
from unidecode import unidecode
from html.parser import HTMLParser
import csv



# # create mySQL connection to the local host using MySQLdb module.We are setting the charset to utf8mb4 to deal with smileys, emoticons, foriegn characters etc
# conn = MySQLdb.connect("localhost","root","mongo1234","mydb1",use_unicode=True, charset="utf8mb4")

# c = conn.cursor()
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

# Twitter's results just give us back the tweet, but don't tell us which keyword it was found with
# so, we have to use a keyword dictionary to search the tweet and match it back up to the party


#exclude punctuations
regex = re.compile('[%s]' % re.escape(string.punctuation))

# Open/create a file to append data to
csvFile = open('hate_final.csv', 'a')
# weetID , created_at, tweet,screen_name,followers_count,friends_count,\
#                                         verified_bit, source,country, country_code,full_name,name, place_type,\
#                                   reply_count, retweet_count, favorite_count
columnTitleRow = " tweetID,dateTime, tweet, screen_name, followers_count,  source, country, country_code, full_name, name, place_type, quote_count, reply_count, retweet_count, favorite_count\n"
csvFile.write(columnTitleRow)

#Use csv writer
csvWriter = csv.writer(csvFile)

#consumer key, consumer secret, access token, access secret which is passed to the oauth for tweepy.
# ckey="nu4okkirMdCPQBUbsph7M4nOo"
# csecret="iN8iNWOayRRJpEOhH9TmprIUO9jPIz511TeFDLTyZ6WOMuQ51x"
# atoken="1068606021185282050-VjaqxC0OA21j5KUtMwohBuB9O9rV7o"
# asecret="mUoPu2a5lIchq6p1XTOi9T7ohjqELbpCKP5MarLNQzhG0"
ckey="guajXxEVFJTvhWaw2oXLKjO7R"
csecret="gW6diAXLsjhYFUaAS2iuzU3nVH3gdIX2YrmLuw0dgKylEAW5bT"
atoken="826497490664894466-mWpnl2Q09Vt8IzOIrqnTJHhcqPmdGeX"
asecret="nmtfzBsij26HGpXxS2ZCadI4mHEIY3l0r22Nb365Yrxk4"



# # this is our SQL for adding the tweetID and results into our DB
# add_tweet = ("INSERT INTO tweets3"
#              "(tweetID, movies_movieName, dateTime, tweet, result, confidence)"
#              "VALUES (%s, %s, %s, %s, %s, %s)")


# Twitter's results just give us back the tweet, but don't tell us which keyword it was found with
# so, we have to use a keyword dictionary to search the tweet and match it back up to the movie
# movieKeyword = dict()
# movieKeyword['aquaman'] = 'Aquaman'
# movieKeyword['bumblebee'] = 'Bumblebee'
# movieKeyword['crimsonpeak'] = 'Crimson Peak'
# movieKeyword['marypoppinsreturns'] = 'Mary Poppins Returns'
# movieKeyword['marypoppins'] = 'Mary Poppins Returns'
# movieKeyword['poppins'] = 'Mary Poppins Returns'
# movieKeyword['vice'] = 'Vice'
# movieKeyword['vicemovie'] = 'Vice'
# movieKeyword['spiderman'] = 'Spiderman into the Spider-Verse'
# movieKeyword['spiderverse'] = 'Spiderman into the Spider-Verse'
# movieKeyword['glassmovie'] = 'Glass'
# movieKeyword['captainmarvel'] = 'Captain Marvel'
# movieKeyword['mortalengines'] = 'Mortal Engines'
# movieKeyword['onceuponadeadpool'] = 'Once Upon a Deadpool'
# movieKeyword['jokermovie'] = 'Joker'
# movieKeyword['joker'] = 'Joker'
# movieKeyword['avengersendgame'] = 'Avengers Endgame'
# movieKeyword['avengers'] = 'Avengers Endgame'
# movieKeyword['endgame'] = 'Avengers Endgame'
# movieKeyword['greenbook'] = 'Greenbook'
# movieKeyword['welcometomarven'] = 'Welcome to Marwen'
# movieKeyword['shazam'] = 'Shazam'
# movieKeyword['themule'] = 'The Mule'




#exclude punctuations
regex = re.compile('[%s]' % re.escape(string.punctuation))

#build the class used to process tweets to check for feeds
class twitter_streaming(StreamListener):

    def on_data(self, data):
        all_data = json.loads(HTMLParser().unescape(data))
        #print(all_data)
        #https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        #https://gist.github.com/hrp/900964
        if 'text' in all_data:
            #1
            tweet = all_data['text']
            tweet = unidecode(tweet)
            #2
            tweetID = all_data['id_str']
            #3
            source = all_data['source']
            source = unidecode(source)
            #4
            if all_data['place']:
                country = all_data['place']['country']
                country = unidecode(country)
                #5
                country_code = all_data['place']['country_code']
                country_code = unidecode(country_code)
                #6
                full_name = all_data['place']['full_name']
                full_name = unidecode(full_name)
                #7
                name =  all_data['place']['name']
                name = unidecode(name)
                #8
                place_type = all_data['place']['place_type']
                place_type = unidecode(place_type)
                #9
            else:
                country = country_code = full_name = name = place_type = "0"
            
            quote_count = all_data['quote_count']
            #10
            reply_count = all_data['reply_count']
            #11
            retweet_count = all_data['retweet_count']
            #12
            favorite_count = all_data['favorite_count']
            #13
            screen_name = all_data['user']['screen_name']
            screen_name = unidecode(screen_name)
            #13
            followers_count = all_data['user']['followers_count']
            #14
            friends_count = all_data['user']['friends_count']
            #15
            verified = all_data['user']['verified']
            #print("verified value is:", verified)
            #type(verified)
        
            
 
            #tweetNoPunctuation = regex.sub('', tweet)
            tweetNoPunctuation = clean_tweet(tweet)
#we want to make sure while compiling tweets, we do not include the oens that are retweeted
            if not all_data['retweeted'] and not tweet.startswith('RT') and 't.co' not in tweet:
               # sentiment_value, confidence = sentiment(tweetNoPunctuation)
                #print(tweet, sentiment_value, confidence) #print output
                
                
                if (verified == True):
                    verified_bit = 1
                    #print("Set")
                else:
                    verified_bit = 0

                
                    
                found = True
#                 party = ""
#                 for word in tweetNoPunctuation.split(" "):  
#                     if word.lower() in party_tags.keys():
#                         party_name = party_tags[word.lower()]
#                         print("Found keyword: ", word, " belongs to party: ", party_name)
#                         found = True
#                         break

                if found:
                    created_at = time.strftime('%Y-%m-%d %H:%M:%S')  
                    newID = (int)(all_data['id'])
                    #twitter JSON is being parsed with queries below and using sentiment module, we are assigning confidence values
                    # tweetID, party_name, dateTime, tweet, source,country, country_code, full_name, name, place_type,\
                    #  reply_count, retweet_count, favorite_count, result, confidence,num_sentiment
                    tweet_data = (tweetID , created_at, tweet,screen_name,followers_count,friends_count,\
                                  verified_bit, source,country, country_code,full_name,name, place_type,\
                                  reply_count, retweet_count, favorite_count)
                    #print(tweet_data)
                    # Write a row to the CSV file. I use encode UTF-8
                    csvWriter.writerow([tweetID , created_at, tweet,screen_name,followers_count,friends_count,\
                                        verified_bit, source,country, country_code,full_name,name, place_type,\
                                  reply_count, retweet_count, favorite_count])
                    
        return True
#     def on_data(self, data):
#         all_data = json.loads(data)
#         if 'text' in all_data:
#             tweet = all_data['text']
#             tweet = unidecode(tweet)
#             tweetNoPunctuation = regex.sub('', tweet)
# #we want to make sure while compiling tweets, we do not include the oens that are retweeted
#             if not all_data['retweeted'] and not tweet.startswith('RT') and 't.co' not in tweet:
#                 sentiment_value, confidence = 1,1
# #             (tweetNoPunctuation)
#                 print(tweet, sentiment_value, confidence)

#                 found = False
#                 movie_name = ""
#                 for word in tweetNoPunctuation.split(" "):  
#                     if word.lower() in movieKeyword.keys():
#                         movie_name = movieKeyword[word.lower()]
#                         print("Found keyword: ", word, " belongs to movie: ", movie_name)
#                         found = True
#                         break

#                 if found:
#                     created_at = time.strftime('%Y-%m-%d %H:%M:%S')  
#                     #twitter JSON is being parsed with queries below and using sentiment module, we are assigning confidence values
#                     tweet_data = (all_data['id_str'], movie_name, created_at, tweet, sentiment_value.lower(), confidence)
# #                     c.execute(add_tweet, tweet_data)
# #                     conn.commit()
                    

#         return True
#error handling, since tweepy tends to time out with twitter with out any reason closing the connection from their side
    def on_limit(self, track):
        print('Limit hit! Track = %s' % track)
        return True

    def on_error(self, status):
        print(status)

    def on_disconnect(self, notice):
        print(notice)


# Twitter Authorization path
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

# create twitter stream, which in turn will start streaming tweeets in JSON format, which we are using to query its metadata and store them separately on to the database
twitterStream = Stream(auth, twitter_streaming())

# Movie keywords  to search for in tweets
#twitterStream.filter()
twitterStream.filter(track=["hate","fuck", "asshole", "bitch", "bomb","nigga" ], languages=["en"])


# cursor.close()
# conn.close()
