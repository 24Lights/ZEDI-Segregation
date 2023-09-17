import os
import pandas as pd

paths=[]


for root, dirs, files in os.walk(r'E:\\ZEDI'):
        for name in files:
            paths.append(os.path.abspath(os.path.join(root, name)))
            
for k in paths :
    df = pd.read_csv(k,low_memory=False)
    upd_df =  df.groupby('Time', as_index=False).first()
    upd_df.to_csv(k, index=False)

print(len(paths))
print(paths)