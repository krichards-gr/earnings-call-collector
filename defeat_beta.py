import defeatbeta_api
from defeatbeta_api.data.ticker import Ticker
ticker = Ticker('TSLA')

transcripts = ticker.earning_call_transcripts()
transcripts.get_transcripts_list()

transcripts = ticker.earning_call_transcripts()
transcripts.get_transcript(2024, 4)