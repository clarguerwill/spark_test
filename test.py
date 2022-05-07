import pandas as pd
import re



df = pd.read_csv("uk_treasury2.csv", header=0)
re_re = lambda s: re.sub(r'[^0-9a-zA-Z_]', "", s).lower()
new_colnames = list(map(re_re, df.columns))
df.columns = new_colnames
df["groupid"].to_csv("test.csv", index=False, header = True)