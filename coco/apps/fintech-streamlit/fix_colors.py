import os
import re

app_dir = "/Users/mgalvis/cocodesktop/fintech-streamlit/app_pages"

old_navy = 'NAVY = ["#0F2B46", "#1B3A5C", "#2E7D8C", "#29B5E8", "#0F4C75", "#3282B8", "#11567F", "#1A5276"]'
new_navy = 'SEMAFORO = ["#264653", "#2A9D8F", "#E9C46A", "#F4A261", "#E63946", "#6A994E", "#386641", "#BC4749"]'

old_heatmap_cs = '[[0,"#FAFBFC"],[1,"#0F2B46"]]'
new_heatmap_cs = '[[0,"#E63946"],[0.35,"#F4A261"],[0.5,"#E9C46A"],[0.75,"#2A9D8F"],[1,"#264653"]]'

fixed = 0
for fname in sorted(os.listdir(app_dir)):
    if not fname.endswith(".py") or fname in ("page_template.py", "conn_helper.py", "map_helper.py", "__init__.py"):
        continue
    fpath = os.path.join(app_dir, fname)
    with open(fpath, 'r') as f:
        content = f.read()
    
    original = content
    
    content = content.replace(old_navy, new_navy)
    content = content.replace("NAVY", "SEMAFORO")
    content = content.replace(old_heatmap_cs, new_heatmap_cs)
    
    if content != original:
        with open(fpath, 'w') as f:
            f.write(content)
        fixed += 1

print(f"Fixed {fixed} files")
