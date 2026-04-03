import http.client, urllib

class Push:

    def __init__(self, config, mensagem):

        conn = http.client.HTTPSConnection("api.pushover.net:443")
        conn.request("POST", "/1/messages.json",
        urllib.parse.urlencode({
            "token": config['token'],
            "user": config['user'],
            "message": mensagem,
        }), { "Content-type": "application/x-www-form-urlencoded" })
        conn.getresponse()
        print("Push enviado com sucesso!")
        print("-->" + config['token'])
        print("-->" + config['user'])
        print("-->" + mensagem)