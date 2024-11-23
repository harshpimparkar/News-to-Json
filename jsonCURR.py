import requests
from bs4 import BeautifulSoup
import pandas as pd
import spacy
from datetime import datetime
import json
from rapidfuzz import process

class DisasterRSSNewsScraper:
    DISASTER_KEYWORDS = [
    # Natural Disasters
    'earthquake', 'seismic', 'tremor',
    'tsunami', 'tidal wave',
    'hurricane', 'cyclone', 'typhoon', 'tropical storm',
    'tornado', 'twister',
    'flood', 'flooding', 'river overflow',
    'landslide', 'mudslide', 'debris flow',
    'volcanic eruption', 'lava flow', 'ash cloud',
    'wildfire', 'bushfire', 'forest fire',
    'avalanche', 'snow slide',
    'drought', 'water scarcity',
    'heatwave', 'extreme heat',
    'blizzard', 'snowstorm', 'ice storm',
    
    # Disaster Response Terms
    'emergency', 'state of emergency',
    'rescue', 'evacuation', 'displacement',
    'relief efforts', 'humanitarian aid',
    'damage assessment', 
    'warning', 'alert', 'advisory',
    
    # Severity Indicators
    'widespread damage', 'destruction', 'casualties',
    'missing persons', 'search and rescue'
]

    def __init__(self, feeds, cities_file):
        self.feeds = feeds
        self.nlp = spacy.load("en_core_web_sm")
        self.cities = self._load_cities(cities_file)

    def _load_cities(self, cities_file):
        cities_df = pd.read_csv(cities_file)
        cities = set()
    
        # Handle case sensitivity and strip whitespace
        if 'City' in cities_df.columns:
            cities.update(cities_df['City'].dropna().str.strip().str.lower())
        
        if 'State' in cities_df.columns:
            cities.update(cities_df['State'].dropna().str.strip().str.lower())
    
        return cities


    def _is_disaster_related(self, text):
        """Check if text contains disaster-related keywords."""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.DISASTER_KEYWORDS)

    def _parse_date(self, date_str):
        try:
            return datetime.strptime(date_str.strip(), '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S')
        except:
            return date_str

    def _extract_locations(self, text):
        
        doc = self.nlp(text)
        ner_locations = {ent.text.lower() for ent in doc.ents if ent.label_ == 'GPE'}
        
        matched_cities = self.cities.intersection(ner_locations)
        matched_states = {state for state in ner_locations if state in self.cities}
        matched_countries = {country for country in ner_locations if country in self.cities}
        
        all_matches = list(matched_cities.union(matched_states, matched_countries))
        
        if all_matches:
            return all_matches
        else:
            return ['Unknown']

    def _fetch_feed(self, url):
        disaster_items = []
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'xml')
            
            for entry in soup.find_all('item'):
                try:
                    title = entry.find('title').text.strip()
                    description = entry.find('description').text.strip()
                    combined_text = f"{title} {description}"
                    
                    # Filter for disaster-related news
                    if self._is_disaster_related(combined_text):
                        disaster_items.append({
                            'title': title,
                            'date': self._parse_date(entry.find('pubDate').text),
                            'description': description,
                            'link': entry.find('link').text.strip(),
                            'locations': self._extract_locations(combined_text),
                            'source': url,
                            'disaster_keywords': [
                                keyword for keyword in self.DISASTER_KEYWORDS 
                                if keyword in combined_text.lower()
                            ]
                        })
                except AttributeError:
                    continue
                    
        except requests.exceptions.RequestException:
            pass
        return disaster_items

    def scrape_disasters(self):
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
        'https://feeds.feedburner.com/ndtvnews-latest',
        'https://zeenews.india.com/rss/india-national-news.xml',
        'https://www.cnbctv18.com/commonfeeds/v1/cne/rss/india.xml',
        'https://services.india.gov.in/feed/rss?cat_id=12&ln=en',
        'https://feeds.bbci.co.uk/news/world/asia/india/rss.xml',
        
    ]
    cities_file = 'states.csv'
    output_file = 'disaster_news2.json'
    
    scraper = DisasterRSSNewsScraper(feeds, cities_file)
    disaster_data = scraper.scrape_disasters()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(disaster_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
