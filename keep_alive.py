# Uptime Reboot dashboard:
# https://uptimerobot.com/dashboard#mySettings

from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Hello. Discord bot UVMCC#6718 is alive!"

def run():
    app.run(host='0.0.0.0',port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()
