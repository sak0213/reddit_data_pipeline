
Here's my Reddit Metrics pipeline tool. At a high-level, it takes an input CSV of Reddit results, then queries & analyzes relevant data about individual Reddit posts and the respective comment sections.

To start, clone the repository:

`git clone [https://github.com/sak0213/reddit_data_pipeline]`
`cd [https://github.com/sak0213/reddit_data_pipeline]`

Create a file named "creds.py" with the following variables:

`client_id = _Reddit API Client ID_
secret_key = _Reddit API Secret Key_
user_agent = _Developer Account Name_`

Install Dependencies

`pip install -r requirements.txt`

Run the pipeline! Keep in mind, line 4 in this script declares the path for our input file:

csv_path = 'reddit_gambling_citation_tracker.csv'

`python start.py`
