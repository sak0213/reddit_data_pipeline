from creds import client_id, secret_key, user_agent
from reddit import RedditManager, OpportunityAssessment

csv_path = 'reddit_gambling_citation_tracker.csv'

praw_creds = {
        'client_id':client_id,
        'secret_key':secret_key,
        'user_agent':user_agent}

reddit_exports = RedditManager(input_csv=csv_path,credential=praw_creds,
        debug=False
    )

opportunity_check = OpportunityAssessment(credential=praw_creds)
