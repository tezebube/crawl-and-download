import requests, queue, threading, _thread, argparse, sys, urllib.parse as urlparse
from pathlib import Path
from bs4 import BeautifulSoup
from time import sleep
from random import randint
from datetime import datetime
from threading import Lock

parser = argparse.ArgumentParser(usage=sys.argv[0] + " <website> [options] <folder>",
	description="Recursively crawl a website and download every file with the selected extension in the href tag (Example: pdf)",
	epilog='''
...				Have fun! 	....	
''')

parser.add_argument('website', type=str, action='store', help='website to crawl in form "http(s)://domain"')
parser.add_argument('-s', '--silent', action='store_true',default=False, help='silent mode - turn off downloading output')
parser.add_argument('-so','--search-only' , action='store_true',default=False, help='just output a list of the files found')
parser.add_argument('-e', '--extension', type=str, action='store',required=False, help='extension to download (dot-less) ex: "pdf"')
parser.add_argument('-es', '--extension-suffix', type=str, action='store',required=False, help='extension suffix in links ex: "?sequence=1&isAllowed=y"')
parser.add_argument('-dp', '--download-prefix', type=str, action='store',required=False, help='URL prefix to download ex: "https://sansorg.egnyte.com/dl/"')
parser.add_argument('-ad', '--accept-url', type=str, action='store',required=False, help='Additional URL prefix to allow link refs: "https://sans.org/"')
parser.add_argument('-o', '--output-dir', type=str, action='store', required=False, help='directory to save files')
parser.add_argument('-t', '--num-threads', type=int, action='store',required=False,default=10, help='number of threads ex 10 (default)')
# requiredNamed = parser.add_argument_group('required named arguments')


# set of links already scanned
SCANNED_LINKS = set()
# set of links match found
TARGET_LINKS = set()

OUTPUT_PARENT_DIR="files"

URL_TIMEOUT_SECS=30
CONFLICT_SLEEP_SECS=10

HTML_EXTENSION="html"
JINA_URL="https://r.jina.ai"

workers = []

n_requests = 0
n_requests_lock = Lock()

def main():
	global workers
	global n_requests

	args = parser.parse_args()

	# if there is no output dir, default to the hostname part of the args.website URL
	if not args.output_dir:
		scheme_split = args.website.split("//")
		sections = scheme_split[1].split("/")
		args.output_dir = sections[0] # hostname
		if args.output_dir == "github.com":
			args.output_dir =  sections[0] + "_" + sections[1]

	output_dir = OUTPUT_PARENT_DIR + "/" + args.output_dir

	print("\nCrawling {} in search of {} files to download into {}".format(args.website,args.extension,output_dir))
	print("\nNumber of N_THREADS: {}".format(args.num_threads))
	print("\nSearch_only: {}\n".format(args.search_only))

	elapsed = datetime.now()

	for i in range(args.num_threads):
		workers.append(worker(
			i, 
			args.website, 
			args.extension, 
			args.extension_suffix,
			args.download_prefix, 
			args.accept_url, 
			output_dir, 
			args.search_only,
			args.silent,
			))

	# Add first URL to start the scanning
	add_link_to_worker_queue(0, args.website)

	for w in workers:
		w.start()

	for w in workers:
		try:
			w.join()
		except KeyboardInterrupt:
			for t in workers:
				t.stop()

	elapsed = datetime.now() - elapsed

	print("\n\n\nFINISHED IN {}.{} seconds.".format(elapsed.seconds, str(elapsed.microseconds/1000)[:3]) )
	print("\t{} TOTAL LINKS SCANNED\n\t{} TOTAL files FOUND".format(len(SCANNED_LINKS), len(TARGET_LINKS)) )
	for link in TARGET_LINKS:
		print("\t\t{}".format(link))
	print("\t{} total web requests.".format(n_requests))

# randomly distribute work along the workers
def add_link_to_queues(link):
	global workers
	workers[randint(0, len(workers)-1 )].add_work(link)

# add work for one specific worker
def add_link_to_worker_queue(index,link):
	global workers
	workers[index].add_work(link)


# save file to disk
def save_file(threadID, file_url, output_dir, file_name):	
	retry_limit = 5
	retry_count = 0
	#check if already exists
	while retry_count < retry_limit:
		if not Path(output_dir + "/" + file_name).is_file():
			try:
				print("[{}] T-ID: [INFO] Downloading {}".format(threadID, file_url))
				r = requests.get(file_url, timeout=URL_TIMEOUT_SECS)
				if r.status_code == 200:
					Path(output_dir).mkdir(parents=True, exist_ok=True)
					with open(output_dir + "/" + file_name, "wb") as f:
						f.write(r.content)
					print("[{}] T-ID: [INFO] saved {}".format(threadID, file_name))
					break
				elif r.status_code == 404:
					print("[{}] T-ID: [ERROR!] 404 Not Found Downloading {}.".format(threadID, file_url))					
					break
				elif r.status_code == 429:
					retry_count += 1
					print("[{}] T-ID: [ERROR!] 429 Conflict Downloading {}. Waiting {} seconds and retrying {} / {}.".format(threadID, file_url, CONFLICT_SLEEP_SECS*retry_count, retry_count, retry_limit))
					sleep(CONFLICT_SLEEP_SECS*retry_count)
					continue
				else:
					retry_count += 1
					print("[{}] T-ID: [ERROR!] {} Downloading {}: {}. Waiting {} seconds and retrying {} / {}.".format(threadID, r.status_code, file_url, CONFLICT_SLEEP_SECS*retry_count, retry_count, retry_limit))
					sleep(CONFLICT_SLEEP_SECS*retry_count)
					continue
			except requests.exceptions.RequestException as e:
				print("[{}] T-ID: [ERROR!] Exception downloading {}: {}".format(threadID, file_url, e))
				break
		else:
			print("[{}] T-ID: [INFO]: {} already exists!".format(threadID, file_name))	

class worker(threading.Thread):
	def __init__(self, threadID, URL, extension, extension_suffix, download_prefix, accept_url, output_dir, search_only, silent):
		super(worker, self).__init__()
		self._stop_event = threading.Event()
		self.URL = URL
		# get the hostname prefix with url scheme
		url_split = URL.split("//")
		self.hostname_prefix = url_split[0] + "//" + url_split[1].split("/")[0]
		self.queue = queue.Queue()
		self.threadID = threadID
		self.terminate = False
		self.fextension = extension
		self.extension_suffix = extension_suffix
		self.download_prefix = download_prefix
		self.accept_url = accept_url
		self.output_dir = output_dir
		self.silent = silent
		self.search_only = search_only
		self.req_ses = requests.session()

	def getID(self):
		return self.threadID

	def get_queue_length(self):
		return self.queue.qsize()

	def add_work(self, link):
		self.queue.put(link)

	def stop(self):
		self.terminate = True
		self._stop_event.set()

	def check_url(self, link):
		return self.URL in link or (self.accept_url and self.accept_url in link)

	def check_if_scanned(self, link):
		global n_requests_lock
		with n_requests_lock:
			present = link in SCANNED_LINKS
			SCANNED_LINKS.add(link)
			return present

	def is_target(self, link):
		if self.fextension:
			extension_to_match = self.fextension + self.extension_suffix if self.extension_suffix else self.fextension
			matches_extension = link[-len(extension_to_match):] == extension_to_match
		else:
			matches_extension = True
		matches_prefix = not self.download_prefix or self.download_prefix in link
		return matches_extension and matches_prefix
		

	# get all the href links from the "link" page
	def get_all_links(self, link):
		global SCANNED_LINKS
		global n_requests
		global n_requests_lock
		retry_limit = 3
		retry_count = 0

		while retry_count < retry_limit:
			try:
				req = self.req_ses.get(link, timeout=URL_TIMEOUT_SECS)
				if req.status_code == 200:
					with n_requests_lock: 
						n_requests += 1
						if not self.silent:
							print("[{}] T-ID: {} scanning {}".format(self.threadID, str(len(SCANNED_LINKS)), link), end="...")
					break
				elif req.status_code == 404:
					print("[{}] T-ID: [ERROR!] 404 Not Found getting HTML for {}. Skipping.".format(self.threadID, link))
					return None
				elif req.status_code == 429:
					retry_count += 1
					print("[{}] T-ID: [ERROR!] 429 Conflict getting HTML for {}. Waiting {} seconds and retrying {} / {}.".format(self.threadID, link, CONFLICT_SLEEP_SECS*retry_count, retry_count, retry_limit))
					sleep(CONFLICT_SLEEP_SECS*retry_count)
					continue
				else:
					retry_count += 1
					print("[{}] T-ID: [ERROR!] {} getting HTML for {}. Waiting {} seconds and retrying {} / {}.".format(self.threadID, req.status_code, link, CONFLICT_SLEEP_SECS*retry_count, retry_count, retry_limit))
					sleep(CONFLICT_SLEEP_SECS*retry_count)
					continue

			except requests.exceptions.MissingSchema:
				#print('invalid url %s' % link)
				return None
			except requests.exceptions.RequestException as e:
				print("[{}] T-ID: [ERROR!]: error getting HTML for {}: {}".format(self.threadID, link, e))
				return None

		print("done.")
		html_soup = BeautifulSoup(req.text, 'html.parser')

		links = [ i.get("href") for i in html_soup.find_all('a') ]
		links = [ e for e in links if e not in SCANNED_LINKS and e is not None and len(e) > 5]
		# file in list? search it and remove from crawling list (if present)
		html_soup.decompose() 	#THIS MADE THE TRICK, NO MORE RAM WASTED!

		# make relative links absolute & drop invalid ones
		for i in range(len(links)):
			if links[i][0] == "/":
				links[i] = self.hostname_prefix + links[i]
			elif links[i][0:7] == "http://":
				links[i] = links[i].replace("http://", "https://")
			elif links[i][0:8] != "https://":
				links[i] = None

		return links

	def run(self):
		global TARGET_LINKS
		retry_limit = 2		

		while not self.terminate:
			link = None
			retry_count = 0
			while retry_count < retry_limit:
				try:
					link = self.queue.get(timeout=1)
					break
				except queue.Empty:
					if retry_count == retry_limit:
						self.stop()
						return
					else:
						retry_count += 1
						print("[{}] T-ID: [INFO] Queue empty. Waiting {} seconds and retrying {} / {}.".format(self.threadID, CONFLICT_SLEEP_SECS*retry_count, retry_count, retry_limit))
						sleep(CONFLICT_SLEEP_SECS*retry_count)
						continue
			
			if link == None:
				continue

			if not self.check_url(link):
				print("[{}] T-ID: [*SKIP] {} is not under the specified DOMAIN.".format(self.threadID, link))
				continue

			# add link to scanned links
			if self.check_if_scanned(link):
				#link already scanned
				#print("[{}] T-ID: [*SKIP] {} already scanned.".format(self.threadID,link))
				continue

			# get all links from the page
			links = self.get_all_links(link)
			if links is None:
				continue

			# check for extensions
			for l in links:
				if l is None:
					continue

				if self.fextension.lower() == HTML_EXTENSION:
					if l not in TARGET_LINKS:
						links.remove(l)
						TARGET_LINKS.add(l)
						print("[{}] T-ID: [INFO] matching link: {}".format(self.threadID, l))
						path = urlparse.urlparse(l).path.strip('/')
						file_name = path.replace('/', '_')
						
						_thread.start_new_thread(save_file, (self.threadID, JINA_URL + "/" + l, self.output_dir, file_name))
				elif self.is_target(l):
					if l not in TARGET_LINKS:
						links.remove(l)
						TARGET_LINKS.add(l)
						print("[{}] T-ID: [INFO] matching link: {}".format(self.threadID, l))
						
						if not self.search_only:
							if self.extension_suffix and l[-len(self.extension_suffix):] == self.extension_suffix:
								l = l[:len(l)-len(self.extension_suffix)]
							file_name = l[l.rfind("/")+1:]
							_thread.start_new_thread(save_file, (self.threadID, l, self.output_dir, file_name))
						continue

				#then crawl each site
				add_link_to_queues(l)

if __name__ == '__main__':
	main()
