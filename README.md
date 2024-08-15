# crawl-and-download
Recursively crawl a website and download every file with the selected extension in the href tag (Example: pdf) 

# To recreate files
source .venv/bin/activate
python ./pdf_downloader.py https://www.sans.org --accept-url=https://www.sans.edu --extension=pdf
python ./pdf_downloader.py https://packetstormsecurity.com --extension=pdf --download-prefix=https://dl.packetstormsecurity.net/
python ./pdf_downloader.py https://github.com/zealraj/Cybersecurity-Books --extension=pdf
python ./pdf_downloader.py https://dspace.mit.edu --extension=pdf --extension-suffix="?sequence=1&isAllowed=y"