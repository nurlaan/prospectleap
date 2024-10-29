import utils.finvizSingleTickerNews as fst
import utils.utils as ut

DBsb = '/home/nurlan/projects/prospect_leap/dev/prospectleap_sandbox.db'
DB = 'prospectleap.db'
ticker = 'ACAD'

ticker_details = fst.finviz_ticker_details(ticker=ticker)
news = ticker_details['news_details']

ut.finviz_table_populate(db_path=DB, data=news)

# print(fst.finviz_ticker_details(ticker))

# Checking status in trackerFinviz table
# status_check = ut.check_dbvalue(db_path=DBsb,
#                           table_name='trackerFinviz',
#                           filter_column='ticker',
#                           filter_value='AACG',
#                           check_column='finvizStatus',
#                           check_value='TODO')
#
# print(status_check)

# Check the ticker
# error_ticker = fst.finviz_ticker_details(ticker=ticker)
# print(error_ticker)
