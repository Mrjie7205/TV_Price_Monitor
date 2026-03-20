@echo off
set PYTHONUNBUFFERED=1
set FEISHU_APP_ID=cli_a914dec64739dcc8
set FEISHU_APP_SECRET=I3KulmKiWbBcbLzf7FNFXgdlOh6L2Una
set FEISHU_APP_TOKEN=M6O1bJfj8aZlo1sKpqqcUWpZnqg
set FEISHU_KEYWORDS_TABLE_ID=tblmtmlbF2Cz2WzL
set FEISHU_NEW_ITEMS_TABLE_ID=tbldE5O7Leg6DdIN
python keywords_monitor.py > run_output.txt 2>&1
echo Done.
