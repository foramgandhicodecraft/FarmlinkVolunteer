import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import pandas as pd
import io, os, time, glob
from io import StringIO 

from pandas.core.frame import DataFrame 

import warnings
warnings.filterwarnings('ignore')

start = time.time()

#Remove intermediate files
mydir = '<Enter your local directory name here>'
filelist = glob.glob(os.path.join(mydir, "*.csv")) 
for f in filelist:
    os.remove(f)

mydir = '<Enter your local directory name here>'

filelist = glob.glob(os.path.join(mydir, "*.csv"))
for f in filelist:
    os.remove(f)

API_KEY = '<enter the mailchimp key>' 
SERVER_PREFIX = 'us10'

def _get_all_campaign_reports():
    """Return a list of dictionaries containing reports. """
    try:
        client = MailchimpMarketing.Client()
        client.set_config({
            "api_key": '<Enter the mailchimp key>',
            "server": SERVER_PREFIX
        })

        #response = client.reports.get_all_campaign_reports()
        response = client.reports.get_all_campaign_reports(count=1000)
        return response
    except ApiClientError as error:
          print("Error: {}".format(error.text))
_get_all_campaign_reports()

def reports_to_pandas():
    """Convert a Mailchimp reports dictionary to a Pandas dataframe."""
    
    reports = _get_all_campaign_reports()
    
    
    df = pd.DataFrame(columns=['id', 'send_time','campaign_title', 'type', 'list_name', 'subject_line', 
                               'preview_text', 'emails_sent', 'abuse_reports', 'unsubscribed', 
                               'hard_bounces', 'soft_bounces', 'syntax_errors', 'forwards_count',
                               'forwards_opens', 'opens_total', 'unique_opens', 'open_rate',
                               'clicks_total', 'unique_clicks', 'unique_subscriber_clicks', 
                               'click_rate', 'list_sub_rate', 'list_unsub_rate', 'list_open_rate', 
                               'list_click_rate', 'total_orders', 'total_revenue',
                               'industrystats_openrate', 'industrystats_clickrate', 'industrystats_bouncerate'
                              ])
    
    if reports:
        for report in reports['reports']: 
            row = {
                'id': report.get('id'),
                'send_time': report.get('send_time'),                
                'campaign_title': report.get('campaign_title'),
                'type': report.get('type'),
                'list_name': report.get('list_name'),
                'subject_line': report.get('subject_line'),
                'preview_text': report.get('preview_text'),
                'emails_sent': report.get('emails_sent'),
                'abuse_reports': report.get('abuse_reports'),
                'unsubscribed': report.get('unsubscribed'),
                'hard_bounces': report.get('bounces').get('hard_bounces'),
                'soft_bounces': report.get('bounces').get('soft_bounces'),
                'syntax_errors': report.get('syntax_errors'),
                'forwards_count': report.get('forwards').get('forwards_count'),
                'forwards_opens': report.get('forwards').get('forwards_opens'),
                'opens_total': report.get('opens').get('opens_total'),
                'unique_opens': report.get('opens').get('unique_opens'),
                'open_rate': report.get('opens').get('open_rate'),
                'clicks_total': report.get('clicks').get('clicks_total'),
                'unique_clicks': report.get('clicks').get('unique_clicks'),
                'unique_subscriber_clicks': report.get('clicks').get('unique_subscriber_clicks'),
                'click_rate': report.get('clicks').get('click_rate'),
                'list_sub_rate': report.get('list_stats').get('sub_rate'),
                'list_unsub_rate': report.get('list_stats').get('unsub_rate'),
                'list_open_rate': report.get('list_stats').get('open_rate'),
                'list_click_rate': report.get('list_stats').get('click_rate'),
                'total_orders': report.get('ecommerce').get('total_orders'),
                'total_revenue': report.get('ecommerce').get('total_revenue'),
                'industrystats_openrate': report.get('industry_stats').get('open_rate'),
                'industrystats_clickrate': report.get('industry_stats').get('bounce_rate'),
                'industrystats_bouncerate': report.get('industry_stats').get('bounce_rate'),

            }
            
            #df = df.append(row, ignore_index=True)
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        
        df = df.fillna(0)
            
        for col in ['emails_sent', 'abuse_reports', 'unsubscribed', 
                    'hard_bounces', 'soft_bounces', 'syntax_errors',
                    'forwards_count', 'forwards_opens', 'opens_total', 
                    'unique_opens', 'open_rate', 'clicks_total', 
                    'unique_clicks', 'unique_subscriber_clicks', 'click_rate',
                    'list_sub_rate', 'list_unsub_rate', 'list_unsub_rate', 
                    'list_open_rate', 'list_click_rate', 'total_orders']:
            df[col] = df[col].astype(int)
            
        df['total_revenue'] = df['total_revenue'].astype(float)
        df['industrystats_openrate'] = df['industrystats_openrate'].astype(float)
        df['industrystats_clickrate'] = df['industrystats_clickrate'].astype(float)
        df['industrystats_bouncerate'] = df['industrystats_bouncerate'].astype(float)

        
        return df

df = reports_to_pandas()


df.to_csv('enter csv file', index=False)


end = time.time()

print('Time taken for program: ', end - start)
