import pandas as pd
from pathlib import Path
df = pd.read_csv('\\\\CHGOLDFS\\Operations\\DEV_Team\\Scorecard\\Scorecard_v2\\Python\\PE_Composite_Final_with_fixedsegments.csv')
df = df[['source_id', 'blockf_id', 'DISTRICT', 'SECTION', 'MIDX', 'MIDY']]
my_columns = ['source_id', 'blockf_id', 'DISTRICT', 'SECTION']
df['blockface_title'] = df.apply(lambda x: '_'.join([str(x[i]) for i in my_columns]), axis=1)
df.to_csv('\\\\CHGOLDFS\\Operations\\DEV_Team\\Scorecard\\Scorecard_v2\\Python\\midpoints.csv', index=False)