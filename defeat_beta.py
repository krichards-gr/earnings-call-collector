import defeatbeta_api
from defeatbeta_api.data.ticker import Ticker
ticker = Ticker('AAPL')

transcripts = ticker.earning_call_transcripts()

transcripts_list = transcripts.get_transcripts_list()
with open('transcripts_list.txt', 'w') as f:
    if hasattr(transcripts_list, 'to_string'):
        f.write(transcripts_list.to_string(max_colwidth=None))
    else:
        f.write(str(transcripts_list))
print("Transcripts list written to transcripts_list.txt")

import json

transcripts = ticker.earning_call_transcripts()
data = transcripts.get_transcript(2024, 4)

with open('transcript_data.json', 'w') as f:
    if hasattr(data, 'to_json'):
        # It's likely a pandas DataFrame
        f.write(data.to_json(orient='records', indent=4))
    else:
        # Try dumping as dict, if it fails, dump as string representation
        try:
            json.dump(data, f, indent=4)
        except TypeError:
            f.write(str(data))
        
print("Data written to transcript_data.json")