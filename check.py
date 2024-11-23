import requests
from bs4 import BeautifulSoup
import pandas as pd
import spacy
from datetime import datetime
import json

class DisasterRSSNewsScraper:
    DISASTER_KEYWORDS = [
        'earthquake', 'tsunami', 'hurricane', 'tornado', 
        'flood', 'cyclone', 'landslide', 'volcanic eruption', 
        'wildfire', 'storm', 'avalanche', 'drought', 
        'natural disaster', 'emergency', 'rescue', 'evacuation'
    ]

    def __init__(self, feeds, cities_file):
        self.feeds = feeds
        self.nlp = spacy.load("en_core_web_sm")
        self.cities = self._load_cities(cities_file)

    def _load_cities(self, cities_file):
        """Load city names from the given file."""
        try:
            cities_df = pd.read_csv(cities_file)
            cities = set()
            for column in ['name', 'country', 'subcountry']:
                if column in cities_df.columns:
                    cities.update(cities_df[column].dropna().str.lower())
            return cities
        except FileNotFoundError:
            print(f"Error: The file '{cities_file}' was not found.")
            return set()
        except Exception as e:
            print(f"Error while loading cities: {e}")
            return set()

    def _is_disaster_related(self, text):
        """Check if the text contains disaster-related keywords."""
        return any(keyword in text.lower() for keyword in self.DISASTER_KEYWORDS)

    def _parse_date(self, date_str):
        """Parse and format date string from RSS feed."""
        try:
            return datetime.strptime(date_str.strip(), '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return date_str

    def _extract_locations(self, text):
        """Extract location names from the text using NLP."""
        doc = self.nlp(text)
        ner_locations = {ent.text.lower() for ent in doc.ents if ent.label_ == 'GPE'}
        matched_locations = self.cities.intersection(ner_locations)
        return list(matched_locations) if matched_locations else ['Unknown']

    def _fetch_feed(self, url):
        """Fetch and parse RSS feed for disaster-related articles."""
        disaster_items = []
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'xml')

            for entry in soup.find_all('item'):
                title = entry.find('title').text.strip() if entry.find('title') else ''
                description = entry.find('description').text.strip() if entry.find('description') else ''
                pub_date = entry.find('pubDate').text.strip() if entry.find('pubDate') else ''
                link = entry.find('link').text.strip() if entry.find('link') else ''

                combined_text = f"{title} {description}"
                if self._is_disaster_related(combined_text):
                    disaster_items.append({
                        'title': title,
                        'date': self._parse_date(pub_date),
                        'description': description,
                        'link': link,
                        'locations': self._extract_locations(combined_text),
                        'source': url,
                        'disaster_keywords': [
                            keyword for keyword in self.DISASTER_KEYWORDS if keyword in combined_text.lower()
                        ]
                    })
        except requests.exceptions.RequestException as e:
            print(f"Error fetching feed from {url}: {e}")
        except Exception as e:
            print(f"Unexpected error processing feed {url}: {e}")
        return disaster_items

    def scrape_disasters(self):
        """Scrape disaster-related articles from all RSS feeds."""
        output = {
            'metadata': {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'feeds': self.feeds,
                'total_locations': len(self.cities)
            },
            'disaster_articles': []
        }

        for feed in self.feeds:
            items = self._fetch_feed(feed)
            output['disaster_articles'].extend(items)

        output['metadata']['total_disaster_articles'] = len(output['disaster_articles'])
        return output

def main():
    feeds = [
        'https://www.thehindu.com/news/national/feeder/default.rss',
        'https://ddnews.gov.in/en/tag/rss/',
        'https://timesofindia.indiatimes.com/rssfeedstopstories.cms',
        'https://feeds.feedburner.com/ndtvnews-latest'
    ]
    cities_file = 'world-cities.csv'
    output_file = 'disaster_news.json'

    scraper = DisasterRSSNewsScraper(feeds, cities_file)
    disaster_data = scraper.scrape_disasters()

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(disaster_data, f, ensure_ascii=False, indent=2)
        print(f"Data successfully saved to {output_file}.")
    except Exception as e:
        print(f"Error saving data to file: {e}")

if __name__ == "__main__":
    main()
