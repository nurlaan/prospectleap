# Obtain total population of stocks from Finviz with price < $20
# Features:
# No.
# Ticker
# Full company namem
# Sector
# Industry
# Country
# Market Cap
# Price
# Float

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

# Finviz Start Page
finviz_start_page = "https://finviz.com/screener.ashx?v=111&f=ind_stocksonly,sh_price_u20&ft=4&o=-price&ar=180"

# Get html content from Finviz
def scrape_finviz_html_content(url): 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
        
    return soup

# Extract last page number 
def extract_last_page_number(html_content):
    
    # Find all 'a' tags with class 'screener-pages'
    page_links = html_content.find_all('a', class_='screener-pages')
    page_links = [link for link in page_links if 'is-next' not in link.get('class', [])]
 
    # Extract the text from the last 'a' tag
    if page_links:
        last_page_text = page_links[-1].text

        # Use regex to extract the number
        match = re.search(r'\d+', last_page_text)
        if match:
            return int(match.group())

# Get html content from Finviz 
html_for_last_page = scrape_finviz_html_content(finviz_start_page)

# Last page number
last_page = extract_last_page_number(html_for_last_page)

# Counter list for Finviz subsequent pages
def finviz_pages(last_page_number):
    pages_list = []
    count = 1
    for page in range(0,last_page_number-1):
        count += 20
        pages_list.append(count)

    return pages_list

finviz_pages_list = finviz_pages(last_page)

# List of Finviz URLs for subsequent pages
def finviz_web_url_list(first_page, pages_list, last_page_number):
    url_list = [first_page]
    for i in range(last_page_number-1):
        counter = pages_list[i]
        url = f"https://finviz.com/screener.ashx?v=111&f=ind_stocksonly,sh_price_u20&ft=4&o=-price&r={counter}&ar=180" 
        url_list.append(url)
    return url_list

finviz_url_list = finviz_web_url_list(finviz_start_page, finviz_pages_list, last_page)


# Parse Finviz html content to list of dictionaries [{date:value, title:value, link:value}]
def companies_summary_list_of_dictionaries(finviz_html_content):
    
    news_table = finviz_html_content.find('table', class_='styled-table-new')
    
    companies = []
    
    if news_table:
        rows = news_table.find_all('tr', class_='styled-row')
        
        # Extract data from each row
        for row in rows:
            cells = row.find_all('td')

            # Extract the required information
            company_data = {
                'No.': cells[0].text.strip(),
                'Ticker': cells[1].text.strip(),
                'Company': cells[2].text.strip(),
                'Sector': cells[3].text.strip(),
                'Industry': cells[4].text.strip(),
                'Country': cells[5].text.strip(),
                'Market Cap': cells[6].text.strip(),
                'Price': cells[8].text.strip(),
                'Float': '0'
            }
            
            companies.append(company_data)
    
    return companies

# Compile list of data per 1 page
def list_of_summaries_per_page():
    per_page_list = []
    
    for i in range(len(finviz_url_list)):
        # Get html content from Finviz
        html_cont = scrape_finviz_html_content(finviz_url_list[i])

        # Parse html into list of dictionaries
        summary = companies_summary_list_of_dictionaries(html_cont)

        # Append to per_page_list
        per_page_list.append(summary)
        
        # Wait for the page to load
        time.sleep(3)
                    
    return per_page_list

# Combine per_page_lists
def combine_per_page_list(pp_list):
    combined_list = []
    
    for i in range(len(pp_list)):
        combined_list += pp_list[i]

    return combined_list

# Run all functions to compile stock population
def compile_stock_population():
    per_page_list = list_of_summaries_per_page()
    final_combined_list = combine_per_page_list(per_page_list)

    return final_combined_list

# Create pandas DataFrame
def stock_population_dataframe(stock_population):    
    return pd.DataFrame(stock_population)

# Compile total stock population
population =  compile_stock_population()

# TEST: Initiate parsed DataFrame
stocks_df = stock_population_dataframe(population)

# Drop duplicates
stocks_df = stocks_df.drop_duplicates(subset=['Ticker'], keep='first')

# Correct counts in No.
stocks_df['No.'] = range(1, len(stocks_df)+1)

# TEST: Export to CSV
stocks_df.to_csv(f"Stocks_population.csv", index=False)

