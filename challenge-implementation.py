import pandas as pd
import numpy as np

# Extraction & Cleansing

excel_file = pd.ExcelFile('data/data.xlsx')
print(excel_file.sheet_names)

carrier_report_raw = excel_file.parse(excel_file.sheet_names[0])
sherpa_report_raw = excel_file.parse(excel_file.sheet_names[1])
tld_report_raw = excel_file.parse(excel_file.sheet_names[2])

## Data loss check function
def data_loss_check(raw, current):
    raw_data_len = len(raw)
    print(f"raw data len: {raw_data_len}")
    current_data_len = len(current)
    print(f"current data len: {current_data_len}")
    data_loss = (raw_data_len - current_data_len) / raw_data_len
    print(f"data loss: {round(data_loss * 100,2)}%")

## TLD Report prep

tld_report = tld_report_raw.copy()

tld_report.drop(columns=['policy_type'], inplace=True)
tld_report['lead_phone'] = tld_report['lead_phone'].astype('Int64').astype('string')
### Identify remaining 'object' and 'int64' columns and cast them to string
for col in tld_report.select_dtypes(include=['object', 'int64']).columns:
    tld_report[col] = tld_report[col].astype('string')

tld_report['lead_first_name'] = tld_report['lead_first_name'].str.upper()
tld_report['lead_last_name'] = tld_report['lead_last_name'].str.upper()

tld_report['lead_language_name'] = tld_report['lead_language_name'].apply(
    lambda x:
      pd.NA if pd.isna(x) else(
        'English' if x == 'en_US' else
        'Spanish' if (x == 'es_ES' or x == 'es_MX')
         else pd.NA
      )
    ).astype('string')

columns_to_consider = ['lead_first_name','lead_last_name','lead_id','policy_id']
tld_report.drop_duplicates(subset=columns_to_consider, inplace=True)
data_loss_check(tld_report_raw,tld_report)


## Sherpa Report prep

sherpa_report = sherpa_report_raw.copy()

sherpa_report = sherpa_report.loc[:,[
    'first_name',
    'last_name',
    'state',
    'phone',
    'email',
    'preferred_language',
    'ffm_app_id',
    'ffm_subscriber_id',
    'issuer_assigned_policy_id',
    'issuer_assigned_subscriber_id',
    'issuer_assigned_primary_member_id',
]]

### Casting types
sherpa_report['phone'] = sherpa_report['phone'].astype('Int64').astype('string')
sherpa_report['ffm_app_id'] = sherpa_report['ffm_app_id'].astype('string')
sherpa_report['ffm_subscriber_id'] = sherpa_report['ffm_subscriber_id'].astype('Int64').astype('string')
for col in sherpa_report.select_dtypes(include=['object']).columns:
    sherpa_report[col] = sherpa_report[col].astype('string')

### Applying UPPER
sherpa_report['first_name'] = sherpa_report['first_name'].str.upper()
sherpa_report['last_name'] = sherpa_report['last_name'].str.upper()

sherpa_report.drop_duplicates(inplace=True)
data_loss_check(sherpa_report_raw,sherpa_report)


## Carrier Report prep

carrier_report = carrier_report_raw.copy()

carrier_report['Phone'] = carrier_report['Phone'].astype('Int64').astype('string')
for col in carrier_report.select_dtypes(include=['object']).columns:
    carrier_report[col] = carrier_report[col].astype('string')

carrier_report.drop_duplicates(inplace=True)
data_loss_check(carrier_report_raw, carrier_report)

## Export clean datasets
tld_report.to_csv('data/tld_report.csv', index=False)
sherpa_report.to_csv('data/sherpa_report.csv', index=False)
carrier_report.to_csv('data/carrier_report.csv', index=False)



# One-Big-Table(OBT) - Implementation

## TLD x Sherpa

sherpa_phone = sherpa_report.copy()
sherpa_app = sherpa_report.copy()

sherpa_phone = sherpa_phone.drop_duplicates(subset=["phone"])
sherpa_app   = sherpa_app.drop_duplicates(subset=["ffm_app_id"])

m1 = pd.merge(left=tld_report, right=sherpa_report, how='left', left_on=['lead_phone','application_number'], right_on=['phone','ffm_app_id'])
m2 = pd.merge(left=tld_report, right=sherpa_phone, how='left', left_on=['lead_phone'], right_on=['phone'])
m3 = pd.merge(left=tld_report, right=sherpa_app, how='left', left_on=['application_number'], right_on=['ffm_app_id'])

tld_sherpa = tld_report.copy()

for col in sherpa_report.columns:
    tld_sherpa[col] = np.where(
        m1[col].notna(), # best match (phone + app)
        m1[col],
        np.where(
            m3[col].notna(), # app match
            m3[col],
            m2[col] # phone match
        )
    )

### Normalizing
    
for col in tld_sherpa.select_dtypes(include=['object']).columns:
    tld_sherpa[col] = tld_sherpa[col].astype('string')

tld_sherpa.drop(columns=['first_name','last_name','phone'], inplace=True)

tld_sherpa.rename(columns={
    'lead_phone':'phone',
    'lead_first_name': 'first_name',
    'lead_last_name': 'last_name',
    'date_converted':'Sold_Date',
    'lead_vendor_name':'vendor_name'
}, inplace=True)

tld_sherpa['language'] = tld_sherpa['lead_language_name'].fillna(tld_sherpa['preferred_language'])
tld_sherpa.drop(columns=['lead_language_name','preferred_language'], inplace=True)

tld_sherpa['new_state'] = tld_sherpa['lead_state'].fillna(tld_sherpa['state'])
tld_sherpa.drop(columns=['lead_state','state'], inplace=True)
tld_sherpa.rename(columns={'new_state':'state'}, inplace=True)

tld_sherpa['ffm_app_id'] = tld_sherpa['ffm_app_id'].fillna(tld_sherpa['application_number'])

tld_sherpa.drop(columns=['application_number'], inplace=True)


## TLD x Sherpa x Carrier

full = (
    tld_sherpa['first_name'].fillna('') + ' ' +
    tld_sherpa['last_name'].fillna('')
).str.strip().astype('string')

tld_sherpa['full_name'] = full.replace('', pd.NA)

final = pd.merge(left=carrier_report,right=tld_sherpa, how='left', left_on=['FullName','Phone'], right_on=['full_name','phone'])

### Normalizing

final.drop(columns=['full_name','first_name','last_name','phone','carrier_name'], inplace=True)

final['First_Name'] = final['FullName'].str.split().str[0]
final['Last_Name'] = final['FullName'].str.split().str[1:].str.join(" ")

final.drop(columns=['FullName'], inplace=True)

final = final.loc[:,['First_Name','Last_Name','Phone','email','lead_id','Carrier','Issuer_Assigned_ID','ffm_app_id','state','language','agent_name','vendor_name','Sold_Date']]

final.rename(columns={
    'email':'Email',
    'lead_id':'Lead_ID',
    'Carrier':'Carrier_Name',
    'Issuer_Assigned_ID':'Policy_ID',
    'ffm_app_id':'FFM_Application_ID',
    'state':'State',
    'language':'Language',
    'agent_name':'Agent_Name',
    'vendor_name':'Vendor_Name'
}, inplace=True)

## Export final dataset

final.to_csv('data/final.csv', index=False)
