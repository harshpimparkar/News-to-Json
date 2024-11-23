from datetime import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import csv

# Define the news websites to scrape
news_sites = [
    'https://www.thehindu.com/news/national/',
    'https://www.indiatoday.in/india',
    'https://www.ndtv.com/india-news',
    'https://www.hindustantimes.com/india-news'
]

# Initialize the Selenium webdriver
driver = webdriver.Chrome()

news_data = []

for site in news_sites:
    driver.get(site)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Find all articles related to natural disasters in India
    articles = soup.find_all('article', string=lambda text: 'India' in text and 'disaster' in str(text).lower())

    for article in articles:
        headline = article.find('h2').text.strip()
        description = article.find('p').text.strip()
        link = article.find('a')['href']
        date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        news_data.append({
            'headline': headline,
            'description': description,
            'link': link,
            'date_time': date_time,
            'source': site
        })

# Save the scraped data to a CSV file
with open('natural_disaster_news.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['headline', 'description', 'link', 'date_time', 'source']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in news_data:
        writer.writerow(row)

driver.quit()