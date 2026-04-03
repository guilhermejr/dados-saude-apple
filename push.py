import http.client, urllib

class Push:

    def mensagem(self, config, mensagem):

        conn = http.client.HTTPSConnection("api.pushover.net:443")
        conn.request("POST", "/1/messages.json",
        urllib.parse.urlencode({
            "token": config['token'],
            "user": config['user'],
            "message": mensagem,
        }), { "Content-type": "application/x-www-form-urlencoded" })
        conn.getresponse()