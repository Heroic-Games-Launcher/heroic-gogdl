import sys
import threading
import json
import logging
from gogdl.dl import dl_utils
from time import sleep, time


class ProgressBar(threading.Thread):
    def __init__(self, max_val, total_readable_size, length):
        self.logger = logging.getLogger('PROGRESS')
        self.downloaded = 0
        self.total = max_val
        self.length = length
        self.started_at = time()
        self.total_readable_size = total_readable_size
        self.completed = False
        
        super().__init__(target=self.print_progressbar)

    def print_progressbar(self):
        done = 0

        while True:
            if(self.completed):
                break
            percentage = (self.downloaded / self.total) * 100
            running_time = time() - self.started_at
            runtime_h = 0
            runtime_m = 0
            runtime_s = 0
            if running_time:
                runtime_h = int(running_time // 3600), 
                running_time = running_time % 3600
                runtime_m = int(running_time // 60)
                runtime_s = int(running_time % 60)
            else:
                runtime_h = runtime_m = runtime_s  = 0
            readable_downloaded = dl_utils.get_readable_size(self.downloaded)
            self.logger.info(f'= Progress: {percentage:.02f} {self.downloaded}/{self.total}, '+
                             f'Runtime: 00, '+
                             'ETA: 00:00:00')
            self.logger.info(f'= Downloaded: {self.downloaded / 1024 / 1024:.02f} MiB')
            sleep(1)
    def update_downloaded_size(self, addition):
        self.downloaded+=addition