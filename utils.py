import weaviate

URL = ""
APIKEY = ""

def connect():

    client = weaviate.connect_to_wcs(
        cluster_url=URL,
        auth_credentials=weaviate.auth.AuthApiKey(APIKEY))

    if client.is_live():
        print("CONNECTED TO WEAVIATE DATABASE")
    else:
        print("UNABLE TO CONNECT TO WEAVIATE DATABASE")
        sys.exit()

    return client
