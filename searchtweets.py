import socket
import requests

# token pour acceder a l'api
bearer_token = "mettre votre token"
search_url = "https://api.twitter.com/2/tweets/search/recent"

# parametres de la recherche
query_params = {'query': '( #morocco #bono ) OR #bounou OR #hakimi OR #boufal','tweet.fields': 'text', 'max_results':'100' }


# ajouter le token dans le header de la requette
def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


# executer la requete
def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# envoyer les textes des tweets par connection tcp
def send_tweets_to_spark(tweets,conn):
    for tweet in tweets:
        tweet = tweet+"\n"
        conn.send(tweet.encode('utf-8'))


# faire la pagination de la recherche des tweets
# et les envoyer par tcp
def get_and_send_tweets(conn):
    for i in range(30):
        json_response = connect_to_endpoint(search_url, query_params)
        next_token = json_response["meta"]["next_token"]
        tweets = json_response["data"] 
        tweets = [str(tweet["text"]) for tweet in tweets]
        query_params["next_token"] = next_token
        send_tweets_to_spark(tweets,conn)



if __name__== "__main__":

    # initialisation du websocket
    new_skt = socket.socket()         
    host = "127.0.0.1"     
    port = 5555                 
    new_skt.bind((host, port))        
    print("Now listening on port: %s" % str(port))

    # creation d'une connection avec un client
    new_skt.listen(5)                 
    c, addr = new_skt.accept()        
    print("Received request from: " + str(addr))

    # faire la recherche et envoyer les textes des tweets par le socket 
    get_and_send_tweets(c)

