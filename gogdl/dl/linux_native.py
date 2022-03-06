# Linux installers downloader - this handles downloading and unpacking it to provided path
# Untill GOG will allow to download those using content system this is the only way
from gogdl.dl import dl_utils, progressbar
from gogdl import constants
import os
import xml.etree.ElementTree as ET
import sys
import json
import subprocess
import logging
import hashlib
import shutil


logger = logging.getLogger('LINUX')

def get_folder_name_from_windows_manifest(api_handler, id):
	builds = dl_utils.get_json(
            api_handler, f'{constants.GOG_CONTENT_SYSTEM}/products/{id}/os/windows/builds?generation=2')

	url = builds['items'][0]['link']
	meta,headers = dl_utils.get_zlib_encoded(api_handler, url)
	install_dir = meta["installDirectory"] if builds['items'][0]['generation'] == 2 else meta['product']['installDirectory']
	return install_dir

def download(id, api_handler, arguments):
	logger.info("Getting folder name from windows manifest")
	folder_name = get_folder_name_from_windows_manifest(api_handler, id)
	install_path = os.path.join(arguments.path, folder_name) if arguments.command == 'download' else str(arguments.path)
	logger.info("Getting downlad info")
	game_details = api_handler.get_item_data(id, ['downloads', 'expanded_dlcs'])

	owned_dlcs = []
	if len(game_details['dlcs']) > 0:
		dlcs = game_details['dlcs']['products']
		if arguments.dlcs:
			for dlc in dlcs:
				if api_handler.does_user_own(dlc['id']):
					owned_dlcs.append(dlc)
	installers = game_details['downloads']['installers']

	if os.path.exists(install_path):
		shutil.rmtree(install_path)
	linux_installers = filter_linux_installers(installers)
	
	
	if(len(linux_installers) == 0):
		logger.error("Nothing do download")
		sys.exit(1)

	download_installer(arguments,linux_installers, api_handler, install_path)

	for dlc in owned_dlcs:
		response = api_handler.session.get(dlc['expanded_link'])
		details = response.json()
		dlc_installers = details['downloads']['installers']
		dlc_linux_installers = filter_linux_installers(dlc_installers)
		download_installer(arguments, dlc_linux_installers, api_handler, install_path,True)
	logger.info("Cleaning up")
	shutil.rmtree(constants.CACHE_DIR)

	logger.info("Done")
	sys.exit(0)

def filter_linux_installers(installers):
	linux_installers = []
	# Filter out linux installers
	for installer in installers:
		if installer['os'] == 'linux':
			linux_installers.append(installer)
	return linux_installers

def download_installer(arguments, linux_installers, api_handler, install_path, is_dlc=False):
	found = None
	for installer in linux_installers:
		if installer['language'] == arguments.lang.split('-')[0]:
			found = installer

	if not found:
		if len(linux_installers) > 1:
			logger.error("Couldn't find language you are looking for")
			sys.exit(1)
		else:
			found = linux_installers[0]
	
	
	if not dl_utils.check_free_space(found['total_size'], constants.CACHE_DIR):
		logger.error("Not enough available disk space")

	# There is one file for linux
	url = found['files'][0]['downlink']
	download = dl_utils.get_json(api_handler, url)
	checksum = api_handler.session.get(download['checksum'])
	md5 = ""
	if checksum.ok and checksum.content:
		checksum = ET.fromstring(checksum.content)
		md5 = checksum.attrib['md5']
	success, path = get_file(download['downlink'], constants.CACHE_DIR, api_handler, md5)
	if(success):
		if md5 and dl_utils.calculate_sum(path, hashlib.md5) != md5:
			logger.warning("Installer integrity invalid, downloading again")
			success, path = get_file(download['downlink'], constants.CACHE_DIR, api_handler, md5)
	unpacked_path = os.path.join(constants.CACHE_DIR, 'unpacked')
	logger.info('Checking available disk space')
	
	if not dl_utils.check_free_space(get_installer_unpack_size(path), unpacked_path):
		logger.error("Not enough available disk space")
		sys.exit(1)
	logger.info("Looks fine continuing")
	logger.info("Unpacking game files")
	unpack_installer(path, unpacked_path, logger)
	
	gamefiles_path = os.path.join(unpacked_path, 'data', 'noarch')
	# Move files to destination
	# shutil.move(gamefiles_path+'/*', install_path)
	command = f'mv -f "{gamefiles_path}" "{install_path}"'
	if is_dlc:
		command = f'cp -r "{gamefiles_path}"/* "{install_path}"'
	logger.info("Moving game files")
	subprocess.run(command, shell=True)

	shutil.rmtree(unpacked_path)
	
def get_installer_unpack_size(script_path):
	# From sharkwouter's minigalaxy code
	var = subprocess.Popen(['unzip', '-v', script_path], stdout=subprocess.PIPE)
	output = var.communicate()[0].decode("utf-8")
	var.wait()
	lines_list = output.split("\n")
	if len(lines_list) > 2 and not lines_list[-1].strip():
		last_line = lines_list[-2].strip()
	else:
	    last_line = lines_list[-1].strip()
	size_value = int(last_line.split()[0])
	return size_value

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
			logger.info("Using existing file")
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

