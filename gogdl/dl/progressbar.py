import threading
import logging
from time import sleep, time


class ProgressBar(threading.Thread):
    def __init__(self, max_val):
        self.logger = logging.getLogger("PROGRESS")
        self.downloaded = 0
        self.total = max_val
        self.started_at = time()
        self.last_update = time()
        self.completed = False

        self.decompressed = 0

        self.downloaded_since_last_update = 0
        self.written_since_last_update = 0
        self.read_since_last_update = 0

        self.read_total = 0
        self.written_total = 0

        super().__init__(target=self.loop)

    def loop(self):
        while not self.completed:
            self.print_progressbar()
            sleep(1)
        self.print_progressbar()
    def print_progressbar(self):
        percentage = (self.written_total / self.total) * 100
        running_time = time() - self.started_at
        runtime_h = int(running_time // 3600)
        runtime_m = int((running_time % 3600) // 60)
        runtime_s = int((running_time % 3600) % 60)

        print_time_delta = time() - self.last_update
        
        average_speed = self.downloaded / running_time
        average_decompress = self.decompressed / running_time
        if print_time_delta:
            current_w_speed = self.written_since_last_update / print_time_delta
            current_r_speed = self.read_since_last_update / print_time_delta
        else:
            current_w_speed = 0
            current_r_speed = 0

        if percentage > 0:
            estimated_time = (100 * running_time) / percentage - running_time
        else:
            estimated_time = 0
        estimated_time = max(estimated_time, 0) # Cap to 0

        estimated_h = int(estimated_time // 3600)
        estimated_time = estimated_time % 3600
        estimated_m = int(estimated_time // 60)
        estimated_s = int(estimated_time % 60)

        self.logger.info(
            f"= Progress: {percentage:.02f} {self.written_total}/{self.total}, "
            + f"Running for: {runtime_h:02d}:{runtime_m:02d}:{runtime_s:02d}, "
            + f"ETA: {estimated_h:02d}:{estimated_m:02d}:{estimated_s:02d}"
        )

        self.logger.info(
            f"= Downloaded: {self.downloaded / 1024 / 1024:.02f} MiB, "
            f"Written: {self.written_total / 1024 / 1024:.02f} MiB"
        )

        self.logger.info(
            f" + Download\t- {average_speed / 1024 / 1024:.02f} MiB/s (raw) "
            f"/ {average_decompress / 1024 / 1024:.02f} MiB/s (decompressed)"
        )

        self.logger.info(
            f" + Disk\t- {current_w_speed / 1024 / 1024:.02f} MiB/s (write) / "
            f"{current_r_speed / 1024 / 1024:.02f} MiB/s (read)"
        )

        self.downloaded_since_last_update = 0
        self.written_since_last_update = 0
        self.read_since_last_update = 0

        self.last_update = time()

    def update_downloaded_size(self, addition):
        self.downloaded += addition
        self.downloaded_since_last_update += addition

    def update_decompressed_size(self, addition):
        self.decompressed += addition

    def update_bytes_written(self, addition):
        self.written_total += addition
        self.written_since_last_update += addition

    def update_bytes_read(self, addition):
        self.read_total += addition
        self.read_since_last_update += addition
