To install dependencies, do:
pip install -r requirements.txt

# Setup
- create a twitter application [here](https://dev.twitter.com/)
- get your API key and secret (consumer keys), and your personal keys (access token and access secret)

## Retrieving Tweets
- Use the config.yaml file to specify your credentials, and which fields you want to filter on.
- Check the [API documentation](https://dev.twitter.com/docs/streaming-apis/parameters) for the possible query parameters
- to start retrieving tweets, run:

'''
python twitterstream.py <path-to-config-file> 
'''

