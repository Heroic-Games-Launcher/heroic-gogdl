# One file downloading functions
from gogdl.dl import dl_utils
import os
import sys


def download(id, api_handler):
	game_details = api_handler.get_game_details(id)
	downloads = game_details['downloads']
	print("Linux downloads are not implemented yet")
	sys.exit(0)
    
	"""
    "downloads": [
		[
			"English",
			{
				"windows": [
					{
						"manualUrl": "\/downloads\/wanderlust_transsiberian\/en1installer0",
						"name": "Wanderlust: Transsiberian",
						"version": "1.1.13.2003271410",
						"date": "",
						"size": "208 MB"
					}
				],
				"mac": [
					{
						"manualUrl": "\/downloads\/wanderlust_transsiberian\/en2installer0",
						"name": "Wanderlust: Transsiberian",
						"version": "1.1.13.2003271410",
						"date": "",
						"size": "216 MB"
					}
				],
				"linux": [
					{
						"manualUrl": "\/downloads\/wanderlust_transsiberian\/en3installer0",
						"name": "Wanderlust: Transsiberian",
						"version": "1.1.13.2003271410",
						"date": "",
						"size": "228 MB"
					}
				]
			}
		]
    """

def get_file(url, path, api_handler):
    print(url)
    response = api_handler.session.get(
        url, stream=True, allow_redirects=True)
    total = response.headers.get('Content-Length')
    total_readable = dl_utils.get_readable_size(int(total))
    file_name = response.url[response.url.rfind("/")+1:response.url.rfind("?")]
    path = os.path.join(path,file_name)
    if os.path.exists(path):
        os.remove(path)
    

    with open(path, 'ab') as f:
        if total is None:
            f.write(response.content)
        else:
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                f.write(data)
                downloaded += len(data)
                progress(downloaded, total, total_readable)
        f.close()
    return response.ok

def progress(downloaded, total, total_readable):
    length = 50
    done = round((downloaded/total) * 50)
    current = dl_utils.get_readable_size(downloaded)

    sys.stdout.write(f'\r[{"█" * done}{"." * (length-done)}] {done*2}% {round(current[0], 2)}{current[1]}/{round(total_readable[0], 2)}{total_readable[1]}')
    sys.stdout.flush()

