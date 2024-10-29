import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Union
from .utils import finviz_table_populate, update_dbvalue

# Get html content from Finviz
def scrape_finviz_html_content(ticker: str) -> BeautifulSoup:
    """
    Gets html content from finviz and returns in raw html code.

    Args:
        ticker: Ticker of the company for which web content is obtained from Finviz

    Returns:
        Web content as a BeautifulSoup object
    """
    url = f"https://finviz.com/quote.ashx?t={ticker}&p=d" 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
        
    return soup

# Parse Finviz html content to list of dictionaries [{date:value, title:value, link:value}]
def news_data(finviz_html_content: BeautifulSoup, tckr: str) -> List[Dict]:
    """
    Gets list of news title and their links from Finviz.

    Args:
        finviz_html_content: Web content as a BeautifulSoup object
        tckr: Ticker of the company
    
    Returns:
        List of news title and their links as dictionaries, which includes tickers, dates, titles and links
    """
    
    news_table = finviz_html_content.find('table', class_='fullview-news-outer')
    
    news_data = []
    
    if news_table:
        rows = news_table.find_all('tr', class_='cursor-pointer')
        for row in rows:
            date_cell = row.find('td', align='right')
            news_cell = row.find('td', align='left')
            
            if date_cell and news_cell:
                date = date_cell.text.strip()
                
                news_link = news_cell.find('a', class_='tab-link-news')
                if news_link:
                    title = news_link.text.strip()
                    link = news_link['href']
                                    
                    news_data.append({
                        'ticker': tckr,
                        'date': date,
                        'title': title,
                        'link': link
                    })
    return news_data

# Extract Shares Float
def extract_shares_float(finviz_html_content: BeautifulSoup) -> Union[str, None]:
    """
    Returns shares float.

    Args:
        finviz_html_content: Web content as a BeautifulSoap object

    Returns:
        Number of shares float as a string
    """
    
    # Find the td containing "Shs Float"
    shares_float_label = finviz_html_content.find('td', class_='snapshot-td2', string='Shs Float')
    
    if shares_float_label:
        # Get the next td element which contains the value
        shares_float_value = shares_float_label.find_next('td')
        if shares_float_value:
            # Extract the text within the <b> tag
            value = shares_float_value.find('b')
            if value:
                return value.text.strip()
    
    return None

# function that returns news details and number of shares float for a single ticker
def finviz_ticker_details(ticker: str) -> Dict[str, Union[List[Dict[str, str]], 'str']]:
    """
    Gets news details (ticker, date, title, link) and number of shares float for a single ticker from Finviz

    Args:
        ticker: Ticker to be processed

    Returns:
        Dictionary: {'news_details':[{'ticker':'str','date':'str', 'title':'str', 'link':'str'}...], 'shares_float':'str'}
    """
    # Get html content
    soup = scrape_finviz_html_content(ticker)
    
    # Get news details
    news_details = news_data(soup, ticker)
    
    # Get shares float
    shares_float = extract_shares_float(soup)

    # Build result
    result = {'news_details':news_details, 'shares_float':shares_float}

    return result

# Process one ticker and populate table 'finviz', update float in table 'companyDetails', and update 'trackerFinviz' table to status 'complete'
def process_finviz_single_ticker(db: str, ticker: str) -> bool:
    """
    Process single ticker and populates 'finviz' table, updates float in 'companyDetails' table from Finviz, and changes status to 'complete' or 'error' in 'trackerFinviz' table

    Args:
        db: Path to the database
        ticker: Ticker to process

    Returns:
        Boolean: If all tables successfully updated it returns True, otherwise false
    """
    finviz_news_details = finviz_ticker_details(ticker)['news_details']

    finviz_shares_float = finviz_ticker_details(ticker)['shares_float']

    # Set flag
    flag = True

    try:
        finviz_table_populate(db_path=db, data=finviz_news_details)
    except Exception as e:
        flag = False

    try:
        update_dbvalue(
            db_path=db,
            table_name='companyDetails',
            search_column='ticker',
            update_column='float',
            search_value=ticker,
            value=finviz_shares_float)
    except Exception as e:
        flag = False

    if flag == True:
        update_dbvalue(
            db_path=db,
            table_name='trackerFinviz',
            search_column='ticker',
            update_column='finvizStatus',
            search_value=ticker,
            value='completed')
    else:
        update_dbvalue(
            db_path=db,
            table_name='trackerFinviz',
            search_column='ticker',
            update_column='finvizStatus',
            search_value=ticker,
            value='error')

    return flag
