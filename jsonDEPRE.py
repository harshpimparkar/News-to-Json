import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import spacy
from datetime import datetime
import json
from typing import List, Dict, Set
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RSSNewsScraper:
    def __init__(self, feeds: List[str], cities_file: str):
        """
        Initialize the RSS scraper with feeds and cities data.
        
        Args:
            feeds: List of RSS feed URLs
            cities_file: Path to the CSV file containing city data
        """
        if not os.path.exists(cities_file):
            raise FileNotFoundError(f"Cities file not found: {cities_file}")
        
        self.feeds = feeds
        self.nlp = spacy.load("en_core_web_sm")
        self.cities = self._load_cities(cities_file)
        
    def _load_cities(self, cities_file: str) -> Set[str]:
        """
        Load and process cities data from CSV.
        
        Args:
            cities_file: Path to the CSV file
        
        Returns:
            Set of unique location names
        """
        try:
            cities_df = pd.read_csv(cities_file)
            
            # Validate required columns
            required_columns = ['name', 'country', 'subcountry']
            missing_columns = [col for col in required_columns if col not in cities_df.columns]
            
            if missing_columns:
                logger.warning(f"Missing columns in cities file: {missing_columns}")
            
            cities = set()
            
            # Process locations from available columns
            for col in required_columns:
                if col in cities_df.columns:
                    cities.update(cities_df[col].dropna().str.lower())
            
            logger.info(f"Loaded {len(cities)} unique locations from {cities_file}")
            return cities
            
        except pd.errors.EmptyDataError:
            logger.error("Cities file is empty")
            return set()
        except Exception as e:
            logger.error(f"Error loading cities file: {e}")
            return set()

    def _parse_date(self, date_str: str) -> str:
        """
        Parse and standardize date format.
        
        Args:
            date_str: Date string to parse
        
        Returns:
            Standardized date string
        """
        try:
            date_obj = datetime.strptime(date_str.strip(), '%a, %d %b %Y %H:%M:%S %z')
            return date_obj.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            return date_str

    def _extract_locations(self, text: str) -> List[str]:
        """
        Extract locations from text using spaCy NER.
        
        Args:
            text: Text to extract locations from
        
        Returns:
            List of matched locations
        """
        doc = self.nlp(text)
        ner_locations = {ent.text.lower() for ent in doc.ents if ent.label_ == 'GPE'}
        matched_locations = self.cities.intersection(ner_locations)
        return list(matched_locations) if matched_locations else ['Unknown']

    def _fetch_feed(self, url: str) -> List[Dict]:
        """
        Fetch and parse a single RSS feed.
        
        Args:
            url: RSS feed URL
        
        Returns:
            List of parsed articles
        """
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
                    
                    combined_text = f"{title} {description}"
                    locations = self._extract_locations(combined_text)
                    
                    items.append({
                        'title': title,
                        'date': date,
                        'description': description,
                        'link': link,
                        'locations': locations,
                        'source': url
                    })
                except AttributeError as e:
                    logger.warning(f"Error parsing entry in {url}: {e}")
                    continue
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching feed {url}: {e}")
        return items

def main():
    # Define RSS feeds and cities file
    feeds = [
        'https://www.thehindu.com/news/national/feeder/default.rss',
        'https://ddnews.gov.in/en/tag/rss/'
    ]
    cities_file = 'world-cities.csv'
    output_file = 'scraped_rss_feed.json'
    
    try:
        # Initialize and run scraper
        scraper = RSSNewsScraper(feeds, cities_file)
        data = scraper.scrape()
        
        # Save to JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Successfully saved {len(data['articles'])} entries to {output_file}")
    
    except Exception as e:
        logger.error(f"Scraping failed: {e}")

if __name__ == "__main__":
    main()