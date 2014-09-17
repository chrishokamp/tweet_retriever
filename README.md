# Setup
```
git clone https://github.com/chrishokamp/tweet_retriever.git
```

- To install dependencies, do:
```
pip install -r requirements.txt
```

- create a twitter application [here](https://dev.twitter.com/)
- get your API key and secret (consumer keys), and your personal keys (access token and access secret)

## Retrieving Tweets
- Use the config.yaml file to specify your credentials, and which fields you want to filter on.
- Check the [API documentation](https://dev.twitter.com/docs/streaming-apis/parameters) for the possible query parameters
- to start retrieving tweets, run:

```
python twitterstream.py <path-to-config-file> 
```
* `nohup python twitter/twitterstream.py twitter/chris.yaml > tweet_crawls/9.6.14-test-crawl.json &`     

## IMPORTANT         
This should be obvious, but DON'T push your application keys and secret to a public repo! Keep your personal config.yaml local.


