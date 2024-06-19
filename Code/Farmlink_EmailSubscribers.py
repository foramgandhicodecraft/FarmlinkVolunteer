import pandas as pd
import io, os, time
from io import StringIO 
from mailchimp3 import MailChimp

import warnings
warnings.filterwarnings('ignore')

start = time.time()

client = MailChimp(mc_api='<enter mailchimp api>',
                   mc_user='<enter mailchimp user name>')

campaign_df = pd.read_csv('enter csv file')

# Sampling 5 rows for testing purposes
campaign_df = campaign_df.sample(n=5)

# Initialize the DataFrame that will store all the results
all_results_df = pd.DataFrame(columns=['campaign_id', 'list_id', 'email_address', 'action', 'timestamp', 'ip'])

for index, row in campaign_df.iterrows():
    idx = row['id']
    email_activity = client.reports.email_activity.all(campaign_id=idx, get_all=True)

    df = pd.DataFrame(columns=['campaign_id', 'list_id', 'email_address', 'action', 'timestamp', 'ip'])

    for email_address in email_activity['emails']:
        if 'activity' in email_address and email_address['activity']:
            for item in email_address['activity']:
                ip = item.get('ip', 0)  # Get 'ip' or default to 0

                row = {
                    'campaign_id': email_address.get('campaign_id'),
                    'list_id': email_address.get('list_id'),
                    'email_address': email_address.get('email_address'),
                    'action': item['action'],
                    'timestamp': item['timestamp'],
                    'ip': ip,
                }
                row_df = pd.DataFrame([row])
                df = pd.concat([df, row_df], ignore_index=True)
        else:
            row = {
                'campaign_id': email_address.get('campaign_id'),
                'list_id': email_address.get('list_id'),
                'email_address': email_address.get('email_address'),
                'action': None,
                'timestamp': None,
                'ip': None,
            }
            row_df = pd.DataFrame([row])
            df = pd.concat([df, row_df], ignore_index=True)

    df = df.fillna(0)
    all_results_df = pd.concat([all_results_df, df], ignore_index=True)

# Save the results to CSV
all_results_df = all_results_df[['campaign_id', 'email_address']]
all_results_df = all_results_df.drop_duplicates(subset=['campaign_id', 'email_address'], keep=False)

output_path = 'C:/Users/foram/OneDrive/Desktop/Farmlink/FinalData/FarmlinkMailChimpCampaignData_EmailSubscribers.csv'
all_results_df.to_csv(output_path, index=False)

end = time.time()
print('Time taken for program: ', end - start)