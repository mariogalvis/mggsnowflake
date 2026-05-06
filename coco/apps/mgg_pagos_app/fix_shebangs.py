import os
import stat

bin_dir = "/Users/mgalvis/cocodesktop/mgg_pagos_app/venv/bin"
old_path = "/Users/mgalvis/mgg_pagos_app/venv"
new_path = "/Users/mgalvis/cocodesktop/mgg_pagos_app/venv"

fixed = 0
for fname in os.listdir(bin_dir):
    fpath = os.path.join(bin_dir, fname)
    if not os.path.isfile(fpath):
        continue
    try:
        with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        if old_path in content:
            content = content.replace(old_path, new_path)
            with open(fpath, 'w', encoding='utf-8') as f:
                f.write(content)
            fixed += 1
    except:
        pass

print(f"Fixed {fixed} files")
