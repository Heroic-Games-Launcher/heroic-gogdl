# Linux installers downloader - this handles downloading and unpacking it to provided path
# Untill GOG will allow to download those using content system this is the only way
from gogdl.dl import dl_utils, progressbar
from gogdl import constants
import os
import xml.etree.ElementTree as ET
import sys
import subprocess
import logging
import hashlib
import shutil

def get_folder_name_from_windows_manifest(api_handler, id):
	builds = dl_utils.get_json(
            api_handler, f'{constants.GOG_CONTENT_SYSTEM}/products/{id}/os/windows/builds?generation=2')

	url = builds['items'][0]['link']
	meta,headers = dl_utils.get_zlib_encoded(api_handler, url)
	return meta["installDirectory"]

def download(id, api_handler, arguments):
	logger = logging.getLogger('LINUX')
	logger.info("Getting folder name from windows manifest")
	folder_name = get_folder_name_from_windows_manifest(api_handler, id)
	install_path = os.path.join(arguments.path, folder_name) if arguments.command == 'download' else str(arguments.path)
	logger.info("Getting downlad info")
	game_details = api_handler.get_item_data(id, ['downloads'])
	installers = game_details['downloads']['installers']
	linux_installers = []
	# Filter out linux installers
	for installer in installers:
		if installer['os'] == 'linux':
			linux_installers.append(installer)
	
	if(len(linux_installers) == 0):
		logger.error("Nothing do download")
		sys.exit(1)

	found = None

	for installer in linux_installers:
		if installer['language'] == arguments.lang.split('-')[0]:
			found = installer

	if not found:
		logger.error("Couldn't find language you are looking for")
		sys.exit(1)
	
	
	if not dl_utils.check_free_space(found['total_size'], constants.CACHE_DIR):
		logger.error("Not enough available disk space")

	# There is one file for linux
	url = found['files'][0]['downlink']
	download = dl_utils.get_json(api_handler, url)
	checksum = api_handler.session.get(download['checksum'])
	checksum = ET.fromstring(checksum.content)
	success, path = get_file(download['downlink'], constants.CACHE_DIR, api_handler, checksum.attrib['md5'])
	if(success):
		print(checksum.attrib['md5'])
		if dl_utils.calculate_sum(path, hashlib.md5) != checksum.attrib['md5']:
			logger.warning("Installer integrity invalid, downloading again")
			success, path = get_file(download['downlink'], constants.CACHE_DIR, api_handler, checksum.attrib['md5'])
	unpacked_path = os.path.join(constants.CACHE_DIR, 'unpacked')
	logger.info("Unpacking game files")
	unpack_installer(path, unpacked_path, logger)
	
	gamefiles_path = os.path.join(unpacked_path, 'data', 'noarch')
	if os.path.exists(install_path):
		shutil.rmtree(install_path)
	# Move files to destination
	command = ['mv', '-f', gamefiles_path, install_path]
	logger.info("Moving game files")
	subprocess.Popen(command)

	logger.info("Cleaning up")
	shutil.rmtree(constants.CACHE_DIR)

	logger.info("Done")
	sys.exit(0)

# Unzips installer to target location
def unpack_installer(script_path, target_path, logger):
    logger.info("Unpacking installer using unzip")
    if os.path.exists(target_path):
        shutil.rmtree(target_path)
    command = ['unzip', '-qq', script_path, '-d', target_path]

    process = subprocess.Popen(command)
    return_code = process.wait()
    return return_code == 1

def get_file(url, path, api_handler, md5):
	response = api_handler.session.get(
		url, stream=True, allow_redirects=True)
	total = response.headers.get('Content-Length')
	total_readable = dl_utils.get_readable_size(int(total))
	file_name = response.url[response.url.rfind("/")+1:response.url.rfind("?")]
	path = os.path.join(path,file_name)
	
	if os.path.exists(path):
		if dl_utils.calculate_sum(path, hashlib.md5) == md5:
			logging.info("Using existing file")
			return True, path
		else:
			os.remove(path)
	
	progress_bar = progressbar.ProgressBar(int(total), dl_utils.get_readable_size(int(total)), 50)
	progress_bar.start()
	with open(path, 'ab') as f:
		if total is None:
			f.write(response.content)
		else:
			total = int(total)
			for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
				f.write(data)
				progress_bar.update_downloaded_size(len(data))
	f.close()
	progress_bar.completed = True
	progress_bar.join()
	return response.ok, path

