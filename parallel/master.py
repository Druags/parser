import csv
from subprocess import Popen, CREATE_NEW_CONSOLE
import time


def split_indexes(indexes, workers):
    k, m = divmod(len(indexes), workers)
    splitted_data = list((indexes[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in list(range(workers))))
    count = 0
    indexes = []
    for split in splitted_data:
        indexes.append(str(start + count) + '-' + str(start + len(split) + count - 1))
        count += len(split)

    return indexes


def main():

    indexes = split_indexes(range(start, end), num_workers)


    processes =[Popen(('py worker.py ' + index), creationflags=CREATE_NEW_CONSOLE) for index in indexes]
    exit_codes = [p.wait() for p in processes]
    print(exit_codes)
    with open('result.csv', 'a', newline='') as out_file:
        for index in indexes:
            with open(f'good_links_{index}.csv', 'r', newline='') as in_file:
                csv_reader = csv.reader(in_file)
                csv_writer = csv.writer(out_file)
                csv_writer.writerows([line for line in csv_reader])


if __name__ == '__main__':
    start = 210000
    end = 230000
    num_workers = 4
    start_time = time.time()
    main()
    end_time = time.time()
    work_time = end_time - start_time
    print(f'Время работы программы: {int(work_time // 3600)}:{int(work_time // 60 % 60)}:{round(work_time % 60, 2)}')
