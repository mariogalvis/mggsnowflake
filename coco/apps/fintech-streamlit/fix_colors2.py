import os

app_dir = "/Users/mgalvis/cocodesktop/fintech-streamlit/app_pages"

old_semaforo = 'SEMAFORO = ["#264653", "#2A9D8F", "#E9C46A", "#F4A261", "#E63946", "#6A994E", "#386641", "#BC4749"]'
new_semaforo = 'SEMAFORO = ["#2D9B2D", "#6ABF4B", "#F5D63D", "#F5A623", "#E63946", "#1B8C1B", "#8BC34A", "#D32F2F"]'

old_heatmap = '[[0,"#E63946"],[0.35,"#F4A261"],[0.5,"#E9C46A"],[0.75,"#2A9D8F"],[1,"#264653"]]'
new_heatmap = '[[0,"#E63946"],[0.25,"#F5A623"],[0.5,"#F5D63D"],[0.75,"#6ABF4B"],[1,"#2D9B2D"]]'

fixed = 0
for fname in sorted(os.listdir(app_dir)):
    if not fname.endswith(".py") or fname in ("conn_helper.py", "map_helper.py", "__init__.py"):
        continue
    fpath = os.path.join(app_dir, fname)
    with open(fpath, 'r') as f:
        content = f.read()
    
    original = content
    content = content.replace(old_semaforo, new_semaforo)
    content = content.replace(old_heatmap, new_heatmap)
    
    if content != original:
        with open(fpath, 'w') as f:
            f.write(content)
        fixed += 1

print(f"Fixed {fixed} files")
