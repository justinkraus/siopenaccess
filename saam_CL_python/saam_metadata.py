from dask.distributed import Client
import dask.bag as db
import json
import s3fs
import time
import pprint
import numpy as np
import pandas as pd

fs = s3fs.S3FileSystem(anon=True)
pp = pprint.PrettyPrinter(indent=4)


##Gets select metadata from SAAM, save to pickle, highlevel analysis

##1 print folders in top location
pp.pprint(fs.ls('smithsonian-open-access/metadata/edan/saam/'))

##2 connect to s3 folder for a single museum
client = Client(processes=False)

b = db.read_text('s3://smithsonian-open-access/metadata/edan/saam/*.txt',
                storage_options={'anon': True}).map(json.loads)

#extract an example json record
saam_example = b.take(1)[0]
with open('saam_metadata_example.json','w') as json_out:
	json.dump(saam_example, json_out, indent=2)

##3 flatten function
def flatten(record):
    """Take a single SAAM metadata record, and pulls out specific pieces of data.

    Parameters
    ----------
    record : dict
        A single SAAM metadata record in highly-nested dictionary format.

    Returns
    -------
    flattened_record: dict
        An un-nested dictionary that only contains the record id, unit code,
        object title, media_count, media_id, topic list, object type, and
        object medium.
    """
    flattened_record = dict()
    flattened_record['id'] = record['id']
    flattened_record['unitCode'] = record['unitCode']
    flattened_record['title'] = record['title']
    media_count = record['content'].get('descriptiveNonRepeating', {}).get('online_media',{}).get('mediaCount',np.nan)
    flattened_record['media_count'] = float(media_count)
    media = record['content'].get('descriptiveNonRepeating', {}).get('online_media',{}).get('media',[])   
    if len(media):
        try: 
          flattened_record['media_id'] = media[0]['idsId']
        except KeyError:
          flattened_record['media_id'] = "N/A"
        
    topics = record['content'].get('indexedStructured',{}).get('topic',[])
    if len(topics):
        flattened_record['topics'] = '|'.join(topics)
    
    if 'freetext' in record['content']:
        if 'objectType' in record['content']['freetext']:
            for obtype in record['content']['freetext']['objectType']:
                if obtype['label'] == 'Type':
                    flattened_record['object_type'] = obtype['content']
        if 'physicalDescription' in record['content']['freetext']:
            for phys in record['content']['freetext']['physicalDescription']:
                if phys['label'] == 'Medium':
                    flattened_record['medium'] = phys['content']
        if 'name' in record['content']['freetext']:
            for name in record['content']['freetext']['name']:
                if name['label'] == 'Artist':
                    flattened_record['artist'] = name['content']
        if 'date' in record['content']['freetext']:
            for date in record['content']['freetext']['date']:
                if date['label'] == 'Date':
                    flattened_record['date'] = str(date['content'])
            
    return flattened_record



saam_json = b.map(flatten).compute()
saam_df = pd.DataFrame(saam_json)
saam_df.to_pickle("./saam_df.pkl")



##4 analysis
saam_df = pd.read_pickle("./saam_df.pkl")

#info
pp.pprint(saam_df.info(verbose=True, null_counts=True))

#common objects and mediums at saam
pp.pprint(saam_df.groupby(['object_type','medium']).size().sort_values(ascending=False).head(20))





