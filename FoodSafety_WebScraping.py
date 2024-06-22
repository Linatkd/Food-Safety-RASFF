#!/usr/bin/env python
# coding: utf-8

# In[ ]:



### __author__ = "Lina Maria Cuervo Diaz"
### __copyright__ = "Copyright (C) 2024 Lina Maria Cuervo Diaz"
### __license__ = "Public Domain"
### __version__ = "1.0"


# # rasff portal

# In[151]:


import requests
import json

result = []
url = "https://webgate.ec.europa.eu/rasff-window/backend/public/notification/search/consolidated/"

for i in range(193): #191
    params = {
        "parameters": {
            "pageNumber": i,
            "itemsPerPage": 100
        }
    }
    req = requests.post(url=url, json=params)
    data = req.json()
    json_data = data['notifications']

    for notification in json_data:  # Changed from 'data' to 'notification' for clarity
        notification_id = notification.get('notifId')
        reference_id = notification.get('reference')
        notifying_country = notification.get('notifyingCountry', {}).get('organizationName', 'not found')
        ecValidationDate = notification.get('ecValidationDate')
        # Extracting origin countries
        origin_countries = []
        for country in notification.get('originCountries', []):
            country_name = country.get('organizationName', 'not found')
            origin_countries.append(country_name)
            
        product_Category = notification['productCategory'].get('description', 'not found')
        product_Type = notification['productType'].get('description', 'not found')
        noti_Class = notification['notificationClassification'].get('description', 'not found')
        risk_Decision = notification['riskDecision'].get('description', 'not found')
        item_link = f'https://webgate.ec.europa.eu/rasff-window/screen/notification/{notification_id}/'

        # Append all extracted data to the result list
        result.append({
            'Notification ID': notification_id,
            'Reference ID': reference_id,
            'Date': ecValidationDate,
            'Notifying Country': notifying_country,
            'Origin Countries': ", ".join(origin_countries), 
            'Product Category': product_Category,
            'Product Type': product_Type,
            'Notification Classification': noti_Class,
            'Risk Decision': risk_Decision,
            'Item Link': item_link
            
        })
        
    time.sleep(1)
    
df_rasff = pd.DataFrame(result)

df_rasff.to_csv('./Recalls Data/recall_rasff_links.csv', index=False)

print('CSV created successfully.')


# In[ ]:





# # Details extraction

# In[ ]:





# In[149]:


import requests
import pandas as pd
import time


# Path to the CSV file that contains the item links
csv_file_path = './Recalls Data/recall_rasff_links.csv'
df_rasff = pd.read_csv(csv_file_path)

result = []

batch_size = 2000  # Size of every batch
sleep_time = 600  # Time between every batch
file_path = './Recalls Data/rasff_measures_data.csv' 

total_links = len(df_rasff['Item Link'])
start_from_batch = 1 
start_index = (start_from_batch - 1) * batch_size

for start in range(start_index, total_links, batch_size):
    end = min(start + batch_size, total_links)
    batch_links = df_rasff['Item Link'][start:end]  

    for link in batch_links:
        api_url = link.replace("/screen/notification/", "/backend/public/notification/view/id/")
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()

            
            notification_data = {
                "Reference": data.get("reference", "Reference not found"),
            }

           
            measures = data.get('product', {}).get('measures', [])
            measure_descriptions = [measure.get('actionTaken', {}).get('description', 'No action description found') for measure in measures]

            notification_data["Measures Taken"] = '; '.join(measure_descriptions) if measure_descriptions else "None"

           
            result.append(notification_data)
        else:
            print(f"Failed to fetch data for URL: {api_url} with status code: {response.status_code}")

    
    df_results = pd.DataFrame(result)
    
    df_results.to_csv(file_path, index=False)
    print(f"Saved batch {start // batch_size + 1} of {total_links // batch_size + 1} to CSV.")
    print(f"Processed links {start + 1} to {end}.")

    
    if end < total_links:
        print(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)

print("All batches processed and data saved.")


# In[ ]:




