import urllib.request

def mk_req( url ):
    return urllib.request.Request( url, headers={"User-Agent": "Altova/1.0"} )
