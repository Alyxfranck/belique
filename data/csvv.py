import pandas as pd
import json

json_file = "contact_data.json"
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

df = pd.DataFrame(json_data)

output_file = "idea.xlsx"
df.to_excel(output_file, index=False, engine='openpyxl')

print(f"Data saved {output_file}")