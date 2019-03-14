import json

with open('scraped_data_news_output_raw.json') as json_file:
    data = json.load(json_file)
    for p in data['newspapers']:
        for key in p:
            value=p[key]
        for article in value['articles']:
            print(article)
