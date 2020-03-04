import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import urllib.robotparser
import os

Queue = ['https://dmoztools.net', 'http://fanacmilan.com/', 'http://riweb.tibeica.com/crawl']
visited = []
LIMIT = 100
CONTENT_DIR = "CONTENT"
ROBOTS = {}


def init():
	if not os.path.isdir(CONTENT_DIR):
		os.makedirs(CONTENT_DIR)


def create_directory(url_parse):
	# check the scheme and create 'http' or 'https' directory
	scheme = url_parse.scheme
	local_path = url_parse.path
	netloc = url_parse.netloc

	# create scheme dir
	dir_path = os.path.join(CONTENT_DIR, scheme)
	if not os.path.isdir(dir_path):
		os.makedirs(dir_path)

	if not local_path:
		ph = os.path.join(CONTENT_DIR, scheme, netloc)
		if not os.path.isdir(ph):
			os.makedirs(ph)
		return os.path.join(CONTENT_DIR, scheme, netloc)

	# check domain and local path directory structure
	basename = os.path.basename(local_path)
	ph = os.path.join(CONTENT_DIR, scheme, netloc, *local_path.split('/'))
	if not os.path.isdir(ph):
		os.makedirs(ph)
	if len(basename.split('.')) > 1: 	# is filename
		return os.path.join(CONTENT_DIR, scheme, netloc, *local_path.split('/'))
	else: 								# is directory
		dir_path = os.path.join(CONTENT_DIR, scheme, netloc, *local_path.split('/'))
		if not os.path.isdir(dir_path):
			os.makedirs(dir_path)

		# check if "path/to/dir" or "path/to/dir/"
		if local_path[-1] == '/':
			return os.path.join(CONTENT_DIR, scheme, netloc, *local_path[:-1].split('/'))  # remove '/' character
		else:
			return os.path.join(CONTENT_DIR, scheme, netloc, *local_path.split('/'))


def get_robots(url):
	# check for Disallow
	# if Allow is present, you should not take it in consideration, because is not included in REP protocol

	url_parse = urlparse(url)
	netloc = url_parse.netloc
	if netloc in ROBOTS:
		rp = ROBOTS[netloc]
	else:
		rp = urllib.robotparser.RobotFileParser()
		rp.set_url("{0}://{1}/robots.txt".format(url_parse.scheme, netloc))
		try:
			rp.read()
			ROBOTS[netloc] = rp
		except:
			return None
	return rp
	

init()


while LIMIT and Queue:
	link = Queue.pop(0)
	print("Current link: {}".format(link))
	robots = get_robots(link)
 
	if not robots or not robots.can_fetch('*', link):
		print("Robots not allowed for link: {}".format(link))
		continue

	url_parse = urlparse(link)

	scheme_domain = "{}://{}".format(url_parse.scheme, url_parse.netloc)
	try:
		data = urllib.request.urlopen(link)
	except:
		continue

	file_name = create_directory(url_parse)

	visited.append(link)
	soup = BeautifulSoup(data.read(), features="html.parser")
	with open(file_name + '.txt', 'wb+') as file:
		file.write(soup.text.encode("utf-8"))

	links = soup.findAll('a')
	for link in links:
		href = link.get('href')
		new_link = urljoin(scheme_domain, href)
		if new_link not in visited:
			Queue.append(new_link)
	LIMIT -= 1

