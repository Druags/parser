import time

from queries import create_tables, fill_tables, add_connections


def main() -> None:
    create_tables()
    fill_tables()
    # add_connections()


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    work_time = end_time - start_time
    print(f'Время работы программы: {int(work_time // 3600)}:{int(work_time // 60 % 60)}:{round(work_time % 60, 2)}')
