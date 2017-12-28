import gzip
import os
import tarfile
import pickle
import time

from six.moves import urllib

from tensorflow.python.platform import gfile


_WIKI_LINK_DATA_URL = "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/wiki-links/"

_IO_TIME = 0
_CPU_TIME = 0


def timing_io(f):
	def wrap(*args):
		global _IO_TIME
		time1 = time.time()
		ret = f(*args)
		time2 = time.time()
		total = (time2 - time1) * 1000.0
		_IO_TIME += total
        
		return ret
	return wrap


def timing_cpu(f):
	def wrap(*args):
		global _CPU_TIME
		time1 = time.time()
		ret = f(*args)
		time2 = time.time()
		total = (time2 - time1) * 1000.0
		_CPU_TIME += total

		return ret
	return wrap


def get_data_file_name(n):
	return "data-0000%d-of-00010" % n


def maybe_download(directory, filename, url):
	"""Download filename from url unless it's already in directory."""
	if not os.path.exists(directory):
		print("Creating directory %s" % directory)
		os.mkdir(directory)
	filepath = os.path.join(directory, filename)
	if not os.path.exists(filepath):
		print("Downloading %s to %s" % (url + filename, filepath))
		filepath, _ = urllib.request.urlretrieve(url + filename, filepath)
		statinfo = os.stat(filepath)
		print("Successfully downloaded", filename, statinfo.st_size, "bytes")
	return filepath


def gunzip_file(gz_path, new_path):
	"""Unzips from gz_path into new_path."""
	print("Unpacking %s to %s" % (gz_path, new_path))
	with gzip.open(gz_path, "rb") as gz_file:
		with open(new_path, "wb") as new_file:
			for line in gz_file:
				new_file.write(line)


def get_wiki_link_data(directory, n):
	"""Download the WMT en-fr training corpus to directory unless it's there."""
	filename = "data-0000%d-of-00010" % n
	data_path = os.path.join(directory, filename)
	if not gfile.Exists(data_path + ".gz"):
		data_file = maybe_download(directory, get_data_file_name(n) + ".gz",
                                 _WIKI_LINK_DATA_URL)
	if not gfile.Exists(data_path):
		gunzip_file(data_path + ".gz", data_path)


@timing_io
def file_get_lines(file_path, mode):
	with open(file_path, mode=mode) as f:
		lines = f.readlines()
	return lines


@timing_cpu
def process_lines(lines):
	wum = {}
	urls = []
	i = -1

	lines = [line.strip() for line in lines]

	for line in lines:
		if line != '':
			words = line.split('\t')
			if words[0] == "URL":
				i += 1
				urls.append(words[1])
				if i % 100000 == 0:
					print("Processing article %d" % i)
			else:
				token = words[1]
				if token in wum:
					wum[token] += (" %d" % i)
				else:
					wum[token] = str(i)
	return wum, urls


@timing_io
def dump_to_pkl(to_wum_path, to_urls_path, wum, urls):
	print("Dumping wum file...")
	with open(to_wum_path, mode="wb") as f:
		pickle.dump(wum, f, pickle.HIGHEST_PROTOCOL)

	print("Dumping ul file...")
	with open(to_urls_path, mode="wb") as f:
		pickle.dump(urls, f, pickle.HIGHEST_PROTOCOL)


@timing_io
def load_from_pkl(wum_path, urls_path):
	print("Loading wum...")
	with open(wum_path, "rb") as f:
		wum = pickle.load(f)

	print("Loading ul...")
	with open(urls_path, "rb") as f:
		urls = pickle.load(f)

	return wum, urls


def create_wum_ul(data_dir, n):
	to_wum_path = os.path.join(data_dir, "%s.wum" % get_data_file_name(n))
	to_urls_path = os.path.join(data_dir, "%s.ul" % get_data_file_name(n))
	file_path = os.path.join(data_dir, get_data_file_name(n))

	if (not gfile.Exists(to_wum_path)) or (not gfile.Exists(to_urls_path)) :
		# with open(file_path, mode="r") as f:
		# 	lines = f.readlines()
		lines = file_get_lines(file_path, "r")

		wum, urls = process_lines(lines)
		
		dump_to_pkl(to_wum_path, to_urls_path, wum, urls)


def prepare_data(data_dir, n):
	get_wiki_link_data(data_dir, n)
	create_wum_ul(data_dir, n)

	wum_path = os.path.join(data_dir, get_data_file_name(n) + ".wum")
	urls_path = os.path.join(data_dir, get_data_file_name(n) + ".ul")

	return load_from_pkl(wum_path, urls_path)
