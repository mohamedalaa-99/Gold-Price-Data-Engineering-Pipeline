import requests
import time

url = "https://api.gold-api.com/price/XAU"

while True:
    data = requests.get(url).json()
    print(data)
    time.sleep(10)