import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime, timedelta
import time
import re

# Clean the the text from whitespaces, newlines and etc.
def clean_text(text):
    # Remove extra whitespace, including newlines and carriage returns
    text = re.sub(r'\s+', ' ', text)
    # Remove any remaining special characters if needed
    text = re.sub(r'[^\w\s.,;!?-]', '', text)
    return text.strip()

# Check if website needs to be scraped by selenium
def is_to_be_scraped_by_selenium(url, domains=["finance.yahoo.com"]):
    return any(domain in url.lower() for domain in domains)

# Get html content from 3d party news provider - with simple BeautifulSoup
def scrape_tp_news_html_content(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    return soup

# Get html content from 3d party news provider - modified with Selenium
def scrape_tp_news_html_content_selenium(url):
   # Set up Chrome options
   chrome_options = Options()
   chrome_options.add_argument("--headless")  # Run in headless mode (no GUI)

   # Set up the Chrome driver
   service = Service(ChromeDriverManager().install())
   driver = webdriver.Chrome(service=service, options=chrome_options)

   try:
       # Navigate to the URL
       driver.get(url)
       
       # Wait for the page to load
       time.sleep(5)

       # Get the page source and parse it with BeautifulSoup
       soup = BeautifulSoup(driver.page_source, 'html.parser')
       return soup

   finally:
       # Always close the driver
       driver.quit()


# Parse 3rd party html content to list of dictionaries [{full_text:value, exact_date:value}]
def tp_news_data(tp_news_html_content):
    soup = tp_news_html_content
    
    news_text = ' '.join([p.text for p in soup.find_all('p')])
    news_text = clean_text(news_text)
    
    results = {}
    
    # Extract the exact date and time
    date_string = soup.find('time')['datetime'] if soup.find('time') else ''
    
    # Parse the date string
    exact_date = date_string

    results['exact_date'] = exact_date
    results['full_text'] = news_text
    
    return results

# Append 3rd party news to Global News List 
def append_tp_news(gl_news_list, news):
    gl_news_list.append(news)

# Create pandas DataFrame from Global News List
def tp_news_dataframe(gl_news_list):    
    return pd.DataFrame(gl_news_list)


