import ibis
import pandas as pd
import time
import sys
from tqdm import tqdm
import utils.utils as ut
import utils.finvizSingleTickerNews as fst

DB = 'prospectleap.db'
DBsb = '/home/nurlan/projects/prospect_leap/dev/prospectleap_sandbox.db'


def main():

    # Get number of tickers to process
    number_of_tickers = get_ticker_count()
    
    # Get the list of tickers to process
    tickers = ticker_list(number_of_tickers)
    print('The below tickers will be proccessed: ')
    print(tickers)
    
     # Actual time start
    start_time = time.time()

    # Calculate and display total expected time
    total_time = number_of_tickers * 6
    print(f"\nTotal estimated time: {total_time} seconds")
    print(f"Processing tickers...\n")

   
    # Execute main function
    progress_bar(tickers)
    
    # Actual end time
    end_time = time.time()

    # Duration
    duration = end_time - start_time

    print(f"\n{number_of_tickers} tickers completed in {duration:.2f} seconds!")



# Get the 'finvizTracker' table
def tracker() -> pd.DataFrame:
    table = ut.get_table(db_path=DB, table_name='trackerFinviz')
    
    return table

# Get the 'finvizTracker' table filtered by 'TODO'
def tracker_filtered_todo(table: pd.DataFrame) -> pd.DataFrame:
    filtered_table = ut.filter_table(df=table, column_name='finvizStatus', filter_values='TODO')
    
    return filtered_table

# Get the number of tickers to process
def get_ticker_count() -> int:
    # Get trackerFinviz Table
    finvizTracker = tracker()

    # Print trackerFinviz status
    print(ut.table_info(
        df = finvizTracker,
        column_name = 'finvizStatus',
        ))

    # trackerFinviz table filtered by 'TODO'
    todo_table = tracker_filtered_todo(finvizTracker)
    todo_count = len(todo_table)

    flag = False
    number_of_tickers = -1
    
    while number_of_tickers < 0 or number_of_tickers > todo_count:
        number_of_tickers = int(input(f"There are {todo_count} TODO tickers. Enter # of tickers to process (6s per ticker). To exit enter '0': "))
        if number_of_tickers == 0:
            sys.exit(0)

    return number_of_tickers

# Get the list of tickers to be processed
def ticker_list(count: int) -> list:
    # Get trackerFinviz Table
    finvizTracker = tracker()

    # trackerFinviz table filtered by 'TODO'
    todo_table = tracker_filtered_todo(finvizTracker)

    # Compile the list
    tickers_list = []

    for i in range(count):
        tickers_list.append(todo_table['ticker'].iloc[i])

    return tickers_list

# Progress bar animation
def progress_bar(tickers: list) -> None:
    num_times = len(tickers)
    ticker_errors = []

    for i in range(num_times):
        current_ticker = tickers[i]

        # Calculate progress percentage
        progress = (i + 1) / num_times
        bar_length = 20
        filled_length = int(bar_length * progress)
        
        # Create the progress bar
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        # Calculate remaining time
        remaining_time = (num_times - (i + 1)) * 6
        
        # Create the status message
        message = f"\rTicker {i+1} - {current_ticker} | Progress: |{bar}| {progress:.1%} | Time remaining: {remaining_time}s"
        
        # Print and flush to ensure single line update
        sys.stdout.write(message)
        sys.stdout.flush()
        
        # Wait for 5 seconds unless it's the last iteration
        if i < num_times - 1:
            time.sleep(5)
    
    # Print newline at the end
    sys.stdout.write('\n')
    sys.stdout.flush()



if __name__ == "__main__":
    main()
