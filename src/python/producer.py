import threading
from client import get_producer, produce_msg
import time

class RepeatTimer(threading.Thread):
    def __init__(self, interval, function, args=None, kwargs=None):
        super().__init__()
        self.interval = interval
        self.function = function
        self.args = args if args else []
        self.kwargs = kwargs if kwargs else {}
        self.finished = threading.Event()

    def run(self):
        while not self.finished.is_set():
            start_time = time.time()
            self.function(*self.args, **self.kwargs)
            elapsed_time = time.time() - start_time
            time.sleep(max(0, self.interval - elapsed_time))

    def cancel(self):
        self.finished.set()

def main():

    num_producers = 10
    target_events_per_second = 10000
    events_per_thread_per_second = target_events_per_second / num_producers
    interval_per_event = 1 / events_per_thread_per_second  # Interval in seconds

    # Create producers
    timers = [
        RepeatTimer(interval_per_event, produce_msg, [i, "SENSOR_DATA", get_producer()])
        for i in range(1, num_producers + 1)
    ]

    try:
        # Start all threads
        for timer in timers:
            timer.start()

        # Let them run for 10 minutes
        time.sleep(600)

    except KeyboardInterrupt:
        pass
    finally:
        # Stop all threads
        for timer in timers:
            timer.cancel()

if __name__ == "__main__":
    main()
