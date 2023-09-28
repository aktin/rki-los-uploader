import pandas as pd


def calc_los(case_data_path):
    with open(case_data_path, 'r') as datafile:
        rows = datafile.read().split('\n')

        data = []
        for row in rows:
            row_list = row.split('	')
            if 'entlassung_ts' in row:
                header_row = dict.fromkeys(row_list)
            else:
                data.append(row_list)

        return


if __name__ == "__main__":
    calc_los("libraries/test_data.txt")
