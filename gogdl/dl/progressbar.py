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
        self.downloaded_since_update = 1
        self.total = max_val
        self.length = length
        self.started_at = time()
        self.last_update = time()
        self.total_readable_size = total_readable_size
        self.completed = False
        self.speed_snapshots = list()

        super().__init__(target=self.print_progressbar)

    def print_progressbar(self):
        done = 0

        while True:
            if(self.completed):
                break
            percentage = (self.downloaded / self.total) * 100
            running_time = time() - self.started_at
            runtime_h = int(running_time // 3600), 
            running_time = running_time % 3600
            runtime_m = int(running_time // 60)
            runtime_s = int(running_time % 60)

            time_since_last_update = time() - self.last_update
            size_left = self.total - self.downloaded

            download_speed = self.downloaded_since_update / max(time_since_last_update,0.1)
            self.speed_snapshots.append(download_speed)
            if len(self.speed_snapshots) > 5:
                self.speed_snapshots.pop(0)
            average_speed = 0
            for snapshot in self.speed_snapshots:
                average_speed += snapshot
            average_speed = average_speed / len(self.speed_snapshots)

            estimated_time = size_left / average_speed

            estimated_h = int(estimated_time // 3600)
            estimated_time = estimated_time % 3600
            estimated_m = int(estimated_time // 60)
            estimated_s = int(running_time % 60)

            self.logger.info(f'= Progress: {percentage:.02f} {self.downloaded}/{self.total}, '+
                            # TODO: Figure out why this line below is throwing an error
                            #  f'Running for: {runtime_h:02d}:{runtime_m:02d}:{runtime_s:02d}, '+
                             f'Running for: 00:00:00, '+
                             f'ETA: {estimated_h:02d}:{estimated_m:02d}:{estimated_s:02d}')
            self.logger.info(f'= Downloaded: {self.downloaded / 1024 / 1024:.02f} MiB')
            self.downloaded_since_update = 1
            sleep(1)
    def update_downloaded_size(self, addition):
        self.downloaded+=addition
        self.downloaded_since_update+=addition