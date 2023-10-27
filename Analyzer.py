import os

import numpy as np
import pandas as pd
from pandas import DataFrame
import datetime
import pytz


def calc_los(broker_export_path):
    # returns a list of filenames that contain results from a export
    def get_all_result_sets():
        exports_url = 'cache/exports'
        result_sets = [f for f in os.listdir(exports_url) if f.__contains__("case_data")]
        return result_sets

        # This function reads all case_data files and adds them together

    def read_data_all(result_set_names):
        data_all = []
        header_row = ""
        for filename in result_set_names:
            directory = broker_export_path + "/" + filename
            with open(directory, 'r') as datafile:
                rows = datafile.read().split('\n')

                _data = []  # data from one hospital

                if 'entlassung_ts' in rows[0]:
                    if header_row == "":    # save column data
                        header_row = dict.fromkeys(rows[0].split('	'))
                        header_row.update({"klinik": None})
                    else:   # check if column data of different origins are the same and can be merged together
                        _header_row = dict.fromkeys(rows[0].split('	'))
                        _header_row.update({"klinik": None})
                        if _header_row != header_row:
                            raise ValueError("Header rows of data files dont match!")
                    rows = rows[1:]

                else:
                    raise ValueError(f"Column Row is missing in {filename}")

                for row in rows:
                    row_list = row.split('	')
                    row_list.append(filename.split("_")[0])
                    _data.append(row_list)

            data_all.extend(_data)  # TODO check if headers of all data chunks are the same

            if len(header_row) < 1:
                raise Exception(
                    f"column \"entlassung_ts\" is required in case data: \"{directory}\", but was not found!")

            if len(data_all) < 1:
                raise Exception(
                    f"do data was found in case data: {directory}")

        return header_row, data_all

    def apply_conversions():
        def convert_time(timestamp: str):
            try:
                time_format = "%Y-%m-%dT%H:%M:%SZ"
                timestamp_obj = datetime.datetime.strptime(timestamp, time_format)
                # TODO set timezone to current
                return timestamp_obj
            except ValueError:
                print(f"A date could not be converted to Datetime: \"{timestamp}\" unsing format \"{time_format.__str__()}\"")
                return None

        def create_columns_year_calweek_calweekyear():
            _col_name = 'aufnahme_ts'
            years_lst = case_data_df.get(_col_name)
            if years_lst is not None:  # check if case_data has a column {_col_name}
                case_data_df['jahr'] = years_lst.apply(
                    lambda date: date.year)  # year from date
                case_data_df['KW'] = years_lst.apply(
                    lambda date: date.strftime("%V"))  # calendar week from date
                case_data_df['kalenderwoche_jahr'] = years_lst.apply(
                    lambda date: date.strftime("%G"))  # year from calendar week
            else:
                raise ValueError(f"case_data is missing column: {_col_name}!")

        def create_column_ersterZ_and_vergleich():  # Todo rename or split function
            case_data_df['ersterZ'] = np.zeros(len(case_data_df))
            case_data_df['vergleich'] = np.zeros(len(case_data_df))
            case_data_df['erster Zeitpunkt'] = np.empty(len(case_data_df))
            case_data_df['LOS'] = np.empty(len(case_data_df))
            # TODO vergleich und ersterZ sind quasi identisch

            for index, row in case_data_df.iterrows():
                if pd.isnull(row['triage_ts']) or row['aufnahme_ts'] <= row['triage_ts']:
                    case_data_df.at[index, 'erster Zeitpunkt'] = row['aufnahme_ts']
                elif row['aufnahme_ts'] > row['triage_ts']:
                    case_data_df.at[index, 'ersterZ'] = 1
                    case_data_df.at[index, 'vergleich'] = 1
                    case_data_df.at[index, 'erster Zeitpunkt'] = row['triage_ts']


                time_diff = case_data_df.at[index, 'entlassung_ts'] - case_data_df.at[index, 'erster Zeitpunkt']

                time_diff_in_mins = time_diff.total_seconds() / 60
                case_data_df.at[index, 'LOS'] = time_diff_in_mins

        for col in case_data_df.columns:
            if col.__contains__("_ts"):
                case_data_df[col] = case_data_df[col].apply(convert_time)

        create_columns_year_calweek_calweekyear()
        create_column_ersterZ_and_vergleich()

    # create dataframe with case data from a case data file
    sets = get_all_result_sets()
    columns, data = read_data_all(sets)
    case_data_df = DataFrame(data=data, columns=columns).dropna()
    apply_conversions()

    return case_data_df


def generate_anzahl_faelle(case_data_df: DataFrame):
    case_count_df = DataFrame(case_data_df['klinik'].value_counts())
    case_count_df.columns = ['klinik', 'Freq']
    return case_count_df


if __name__ == "__main__":
    case_data_df = calc_los("cache/exports")
    case_count_df = generate_anzahl_faelle(case_data_df)
    print()