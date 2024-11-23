import requests
from bs4 import BeautifulSoup
import pandas as pd
import spacy
from datetime import datetime
from typing import List, Dict, Set
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class RSSNewsScraper:
    def __init__(self, feeds: List[str], cities_file: str):
        """
        Initialize the RSS scraper with feeds and cities data.
        
        Args:
            feeds: List of RSS feed URLs
            cities_file: Path to the CSV file containing city data
        """
        self.feeds = feeds
        self.nlp = spacy.load("en_core_web_sm")
        self.cities = self._load_cities(cities_file)
        
    def _load_cities(self, cities_file: str) -> Set[str]:
        """Load and process cities data from CSV."""
        try:
            cities_df = pd.read_csv(cities_file)
            # Create a set of lowercase city names and subcountry names
            cities = set()
            
            # Add city names
            if 'name' in cities_df.columns:
                cities.update(cities_df['name'].dropna().str.lower())
            
            # Add subcountry names
            if 'subcountry' in cities_df.columns:
                cities.update(cities_df['subcountry'].dropna().str.lower())
                
            logging.info(f"Loaded {len(cities)} unique locations from {cities_file}")
            return cities
            
        except Exception as e:
            logging.error(f"Error loading cities file: {e}")
            return set()

    def _parse_date(self, date_str: str) -> str:
        """Parse and standardize date format."""
        try:
            date_obj = datetime.strptime(date_str.strip(), '%a, %d %b %Y %H:%M:%S %z')
            return date_obj.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logging.warning(f"Error parsing date {date_str}: {e}")
            return date_str

    def _extract_locations(self, text: str) -> str:
        """Extract locations from text using spaCy NER."""
        doc = self.nlp(text)
        ner_locations = {ent.text.lower() for ent in doc.ents if ent.label_ == 'GPE'}
        matched_locations = self.cities.intersection(ner_locations)
        return ', '.join(matched_locations) if matched_locations else 'Unknown'

    def _fetch_feed(self, url: str) -> List[Dict]:
        """Fetch and parse a single RSS feed."""
        items = []
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'xml')
            
            for entry in soup.find_all('item'):
                try:
                    title = entry.find('title').text.strip()
                    description = entry.find('description').text.strip()
                    link = entry.find('link').text.strip()
                    date = self._parse_date(entry.find('pubDate').text)
                    
                    # Extract location from both title and description
                    combined_text = f"{title} {description}"
                    location = self._extract_locations(combined_text)
                    
                    items.append({
                        'Title': title,
                        'Date': date,
                        'Description': description,
                        'Link': link,
                        'Location': location,
                        'Source': url
                    })
                except AttributeError as e:
                    logging.warning(f"Error parsing entry in {url}: {e}")
                    continue
                    
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching feed {url}: {e}")
        return items

    def scrape(self) -> pd.DataFrame:
        """Scrape all RSS feeds and return results as DataFrame."""
        all_items = []
        for feed in self.feeds:
            logging.info(f"Scraping feed: {feed}")
            items = self._fetch_feed(feed)
            all_items.extend(items)
            
        df = pd.DataFrame(all_items)
        return df

    def save_to_csv(self, df: pd.DataFrame, output_file: str):
        """Save results to CSV file."""
        try:
            df.to_csv(output_file, index=False, encoding='utf-8')
            logging.info(f"Successfully saved {len(df)} entries to {output_file}")
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")

def main():
    # Define RSS feeds and cities file
    feeds = [
        'https://www.thehindu.com/news/national/feeder/default.rss',
        'https://ddnews.gov.in/en/tag/rss/'
    ]
    cities_file = 'world-cities.csv'
    output_file = 'scraped-rss.csv'
    
    # Initialize and run scraper
    scraper = RSSNewsScraper(feeds, cities_file)
    df = scraper.scrape()
    scraper.save_to_csv(df, output_file)

if __name__ == "__main__":
    main()