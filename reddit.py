import praw
import pandas as pd
from praw.models import Submission
from datetime import datetime, timedelta
import hashlib
import statistics

class RedditManager:
    def __init__(self, input_csv:str, credential:dict, debug:bool = False):
        self.input_df = pd.read_csv(input_csv)
        self.subreddit_search = 'all'
        if debug == True:
            debug_limit = 15
            self.input_df = self.input_df.iloc[:debug_limit]
        self.init_praw_connection(credential)
        self.create_comment_timeframe_lookbacks()
        # self.create_subreddit_list()
        self.resolve_missing_citations()
        self.retry_missing_citations()
        self.create_threads_df()


    def create_comment_timeframe_lookbacks(self):
        current_time = datetime.now()
        time_delta_24h = timedelta(hours=24)
        time_delta_72h = timedelta(hours=72)
        
        self.timestamp_24h_ago = current_time - time_delta_24h
        self.timestamp_72h_ago = current_time - time_delta_72h


    def query_submission(self, indx:int = 0):
        test_url = self.input_df.iloc[indx]['permalink']
        
        return self.reddit.submission(url=test_url)


    def init_praw_connection(self,credential:dict):
        self.reddit = praw.Reddit(
            client_id = credential['client_id'],
            client_secret = credential['secret_key'],
            user_agent = credential['user_agent']
        )


    def parse_thread(self, sub:Submission):
        subreddit = sub.subreddit.display_name
        title = sub.title
        try:
            author = hashlib.sha256(sub.author.name.encode('utf-8')).hexdigest()
        except(AttributeError):
            author = 'deleted'
        creation_time = datetime.fromtimestamp(sub.created_utc)
        score = sub.score
        num_comments = sub.num_comments
        upvote_ratio = sub.upvote_ratio
        awards = sub.distinguished
        nsfw = sub.over_18
        locked = sub.locked

        return subreddit, title, author,creation_time,score,num_comments,upvote_ratio,awards,nsfw,locked
    

    def parse_comments(self, comment):
        try:
            comment_length = len(comment.body.split())
            score = comment.score
            try:
                author = comment.author.name
                deleted = 0
            except(AttributeError):
                author = 'deleted'
                deleted = 1
            created_time = datetime.fromtimestamp(comment.created_utc)

            return comment_length, score, author, deleted, created_time
        except(AttributeError):
            next


    def create_comment_list(self, sub:Submission):

        return sub.comment_list()
    

    def create_subreddit_list(self):

        def parse_subreddit(link):
            try:
                subreddit = link.split('/')[4]
                return subreddit
            
            except IndexError:
                return 'n/a'


        sub_df = self.input_df.copy()
        sub_df['subreddit'] = sub_df['permalink'].apply(parse_subreddit)
        sub_df = sub_df.loc[sub_df['subreddit'] != 'n/a']
        sub_list = sub_df['subreddit'].value_counts().index.to_list()
        if len(sub_list) > 100:
            sub_list = sub_list[:100]
        self.subreddit_search = "+".join(sub_list)


    def search_for_link(self, search_query:str, retry:bool= False):
        if retry==False:
            search_results = self.reddit.subreddit(self.subreddit_search).search(
                query=f'"{search_query}"',
                sort = 'relevance',
                limit = 10,
                syntax = 'cloudsearch')
        if retry==True:
            search_results = self.reddit.subreddit(self.subreddit_search).search(
                query=f'{search_query}',
                sort = 'relevance',
                limit = 10,
                syntax = 'lucene')            
            
        for i in search_results:
            if (i.title).strip().lower() == search_query.lower():
                result = i
            # result = i
            else:
                next
        try:
            return f"https://www.reddit.com{result.permalink}"

        except:
            return 'no relevant result'

        
    def resolve_missing_citations(self):

        for i in range(len(self.input_df)):
            if isinstance(self.input_df.iloc[i]['permalink'], float):
                
                search = f"{self.input_df.iloc[i]['title']}"
                self.input_df.loc[i,'permalink'] = self.search_for_link(search)

            else:
                next


    def retry_missing_citations(self):
        self.create_subreddit_list()
        for i in range(len(self.input_df)):
            if self.input_df.iloc[i]['permalink'] == 'no relevant result':

                search = f"{self.input_df.iloc[i]['title']}"
                self.input_df.loc[i,'permalink'] = self.search_for_link(search, True)

            else:
                next

    def create_thread_id(self):
                
        def parse_post_id(link):
            try:
                id = link.split('/')[6]
                return id
            
            except IndexError:
                return 'n/a'

        self.input_df['post_id'] = self.input_df['permalink'].apply(parse_post_id)

    def parse_comment_df(self, df):
            sum_comments = len(df)
            avg_comments = df['length_in_words'].mean()
            med_comments = df['length_in_words'].median()
            count_40_comments = len(df.loc[df['length_in_words']>= 40])
            max_score = df['length_in_words'].max()
            unique_authors = df['author'].nunique()
            try:
                perct_delted = df['deleted'].sum()/len(df)
            except(ZeroDivisionError):
                perct_delted = 0
            last_comment = df['time'].max()


            return sum_comments, avg_comments,med_comments,count_40_comments,max_score,unique_authors,perct_delted,last_comment
    
    def create_threads_df(self):
        self.create_thread_id()

        thread_list = []
        threads_df_cols = ['id',
            'keyword',
            'reddit_url',
            'subreddit',
            'title',
            'author_has',
            'created_utc',
            'score',
            'num_comments',
            'upvote_ratio',
            'awards_count',
            'locked',
            'over_18',
            'last_activity_utc',
            'age_days',
            'recent_comments_24h',
            'recent_comments_72h']
        
        comments_summary_list = []
        comment_summary_df_cols = [
            'id',
            'reddit_url',
            'total_comments',
            'avg_comment_len_words',
            'median_comment_len_words',
            'pct_comments_ge_40w',
            'top_comment_score',
            'unique_commenters',
            'removed_or_deleted_pct',
            'last_comment_utc']

        for i in range(len(self.input_df)):
            comment_df_results_list = []
            submission = self.query_submission(i)

            # thread features
            thread_id = submission.id
            thread_keyword = self.input_df.loc[i, 'keyword']
            thread_url = self.input_df.loc[i, 'permalink']

            # clean any user permalinks
            # here i'll add code to clean any user-based permalinks. need to decide output style

            all_comments = submission.comments.list()

            for comment in all_comments:
                comment_df_results_list.append(self.parse_comments(comment))
            comment_df = pd.DataFrame(comment_df_results_list, columns=[
                'length_in_words',
                'score',
                'author',
                'deleted',
                'time'])

            # comment details for thread
            thread_last_comment = comment_df['time'].max()
            thread_age_post = datetime.now() - datetime.fromtimestamp(submission.created_utc)
            thread_age_post = thread_age_post.days
            thread_recent_coms_24 = len(comment_df.loc[comment_df['time']>=self.timestamp_24h_ago])
            thread_recent_coms_72 = len(comment_df.loc[comment_df['time']>=self.timestamp_72h_ago])
            comments_summary_list.append(((thread_id, thread_url, *self.parse_comment_df(comment_df))))
            # print((thread_id, thread_keyword, thread_url,*self.parse_thread(submission), thread_last_comment, thread_age_post,thread_recent_coms_24,thread_recent_coms_72))
            thread_list.append((thread_id, thread_keyword, thread_url,*self.parse_thread(submission), thread_last_comment, thread_age_post,thread_recent_coms_24,thread_recent_coms_72))

        threads_df = pd.DataFrame(data=thread_list, columns=threads_df_cols)
        threads_df = threads_df.set_index('id')
        threads_df['is_active_recently'] = threads_df['last_activity_utc'] <= self.timestamp_24h_ago
        threads_df.to_csv('threads.csv')
        comments_summary_df = pd.DataFrame(comments_summary_list, columns=comment_summary_df_cols)
        comments_summary_df = comments_summary_df.set_index('id')
        comments_summary_df.to_csv('comments_summary.csv')

class OpportunityAssessment:
    def __init__(self, credential):
        threads_csv = 'threads.csv'
        thread_df = pd.read_csv(threads_csv)
        comments_csv = 'comments_summary.csv'
        comment_df = pd.read_csv(comments_csv)
        self.analysis_df = pd.merge(thread_df, comment_df, how ='left', on='id')
        self.analysis_df['last_comment_utc'] = pd.to_datetime(self.analysis_df['last_comment_utc'])

        self.init_praw_connection(credential)
        self.create_comment_timeframe_lookbacks()
        self.create_path_recco()
        self.output_opp_csv()

    def create_comment_timeframe_lookbacks(self):
        current_time = datetime.now()
        time_delta_72h = timedelta(hours=72)

        self.timestamp_72h_ago = current_time - time_delta_72h

    def init_praw_connection(self,credential:dict):
        self.reddit = praw.Reddit(
            client_id = credential['client_id'],
            client_secret = credential['secret_key'],
            user_agent = credential['user_agent']
        )

    def query_subreddit_median_score(self, subreddit):
        sub = self.reddit.subreddit(subreddit)
        scores = [submission.score for submission in sub.new(limit=100)]
        return statistics.median(scores)
    
    def check_post_age(self, row):
        age_status = self.analysis_df.loc[row, 'age_days'] <= 3
        return age_status
    
    def check_comment_recency(self, row):
        comment_recency = self.timestamp_72h_ago <= self.analysis_df.loc[row, 'last_comment_utc']
        return comment_recency
    
    def opportunity_scan(self, row):
        if self.check_post_age(row) == True:
            status = 'Age <= 3 days'

            if self.check_comment_recency(row) == True:
                status = f'1: {status} with recent comments'
                reaction = 'Hijack'
            
            elif self.analysis_df.loc[row, 'score'] >= self.query_subreddit_median_score(self.analysis_df.loc[row, 'subreddit']):
                status = f'2: {status} with above-median score'
                reaction = 'Hijack'

            else:
                status = f'0: {status} but poor comment recency or score'
                reaction = 'Alternative'
        
        else:
            status = 'post older than 3 days'
            reaction = 'Alternative'

        return status, reaction

    def create_path_recco(self):
        reactions = []
        reasons = []
        for i in range(len(self.analysis_df)):
            outputs = self.opportunity_scan(i)
            reactions.append(outputs[1])
            reasons.append(outputs[0])

        self.analysis_df['path_reco'] = reactions
        self.analysis_df['reason'] = reasons

    def output_opp_csv(self):
        self.analysis_df = self.analysis_df.rename({'reddit_url_x':'reddit_url'}, axis=1)
        output_df = self.analysis_df[['keyword', 'subreddit', 'title', 'reddit_url', 'score', 'num_comments', 'age_days', 'recent_comments_72h', 'avg_comment_len_words', 'top_comment_score', 'path_reco', 'reason']]
        output_df.to_csv('opportunities.csv',index=False)