import argparse
import json
import logging
import multiprocessing
import os
import re
from time import sleep
LOGGER_FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

arg_parser = argparse.ArgumentParser(
    description='Aggregate queries from log files in given log directory and writes results in given output file')
arg_parser.add_argument('--directory', '-d', type=str, help='path to log directory', required=True)
arg_parser.add_argument('--output', '-o', type=str, help='path to output file', default='output.txt')
arg_parser.add_argument('--processors', '-p', type=int, help='number of processors to be used', default=8)


class Reader:
    """
    Reader class to read and aggregate statistic from log file
    """

    def __init__(self, filename, queue):
        """
        :param filename: str - name of currently processed file
        :param queue: multiprocessing.Queue - queue to hold results
        """
        self.filename = filename
        self.queue = queue
        self.statistic = {'valid': {}, 'non_valid': {}}

    @staticmethod
    def get_id_from_query(query_str):
        """
        read all ids from query string and returns it as set to avoid duplicated ids
        :param query_str: str - query sting from query
        :return: None
        """
        ids = set()
        for data in query_str.split('&'):
            tmp = data.split('=')
            if tmp[0] == 'id':
                ids.add(int(tmp[1]))
        return ids

    @staticmethod
    def get_day_time(timestamp):
        """
        :param timestamp: int - UNIX timestamp
        :return: int - UNIX timestamp as day (UTC)
        """
        return timestamp - timestamp % 86400

    def process_query(self, data):
        """
        extend current statistic with data from current query
        :param data: dict - current query
        :return: None
        """
        query_ids = self.get_id_from_query(data['query_string'])
        is_valid = 'valid' if set(data['ids']) == query_ids else 'non_valid'
        day = self.get_day_time(data['timestamp'])
        if day not in self.statistic[is_valid]:
            self.statistic[is_valid][day] = {'create': 0, 'update': 0, 'delete': 0}
        try:
            self.statistic[is_valid][day][data['event_type']] += 1
        except Exception as e:
            log.error("Error while processing 'event_type': {}, file: {}".format(e, self.filename))

    def read_log(self):
        """
        read log file line by line
        :return: dict - query
        """
        with open(self.filename, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    yield data
                except Exception as e:
                    log.error("Error while reading line in log file: {}, file: {}".format(e, self.filename))

    def start(self):
        """
        process log file
        :return: None
        """
        log.info("Processing '{}'".format(self.filename))
        sleep(2)
        for data in self.read_log():
            try:
                self.process_query(data)
            except Exception as e:
                log.error("Error while reading line in log file: {}, file: {}".format(e, self.filename))
        self.queue.put(self.statistic)
        log.info("Processing '{}' finished".format(self.filename))


def is_log_file(filename):
    """
    Check file to be log file
    :param filename: str
    :return: bool
    """
    if re.fullmatch(r'^\d+.log$', filename):
        return True
    return False


def process_files(files, queue):
    """

    :param files: list - list of files to be processed in multiprocessing
    :param queue: multiprocessing.Queue
    :return: list - list of dictionaries with collected data
    """
    jobs = []
    data = []
    for cur_file in files:
        r = Reader(os.path.join(cur_file), queue)
        p = multiprocessing.Process(target=r.start)
        p.start()
        jobs.append(p)
    for p in jobs:
        data.append(queue.get())
        p.join()
    return data


def update_result(res, new_data):
    """
    update current result dictionary with newly collected
    :param res: dict
    :param new_data: dict
    :return:
    """
    if not res:
        res = new_data
        return res
    for is_valid, days in new_data.items():
        for day, events in days.items():
            if day not in res[is_valid]:
                res[is_valid][day] = events
                continue
            for event, quantity in events.items():
                res[is_valid][day][event] += quantity
    return res


def main(directory, output, processors):
    """
    main code here. Scan directory via os.scandir (to use iterator) and process batch of log files, where size of batch
    equal to quantity of processors passed with sys.args
    :param directory: str - name of log files directory
    :param output: str - name of output file
    :param processors: int - quantity of workers to process simultaneously
    :return: None
    """
    result = {}

    queue = multiprocessing.Queue()
    files_to_process = []

    for file in os.scandir(directory):
        if not is_log_file(file.name):
            continue
        files_to_process.append(file)
        if len(files_to_process) < processors:
            continue
        for data in process_files(files_to_process, queue):
            result = update_result(result, data)
            files_to_process = []
    if files_to_process:
        for data in process_files(files_to_process, queue):
            result = update_result(result, data)

    with open(output, 'w') as f:
        f.write(json.dumps(result, indent=4, sort_keys=True))


if __name__ == '__main__':
    args = arg_parser.parse_args()
    main(args.directory, args.output, args.processors)
