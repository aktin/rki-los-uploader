import os

import numpy as np
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

                data = []  # data from one hospital
                for row in rows:
                    row_list = row.split('	')
                    if header_row == "" and 'entlassung_ts' in row:
                        header_row = dict.fromkeys(row_list)
                        header_row.update({"klinik": None})
                    else:
                        row_list.append(filename.split("_")[0])
                        data.append(row_list)

            data_all.extend(data)  # TODO check if headers of all data chunks are the same

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
                raise ValueError(
                    f"A date could not be converted to Datetime: \"{timestamp}\" unsing format \"{time_format}\"")

        def create_columns_year_calweek_calweekyear():
            _col_name = 'aufnahme_ts'
            years_lst = case_data.get(_col_name)
            if years_lst is not None:  # check if case_data has a column {_col_name}
                case_data['jahr'] = years_lst.apply(
                    lambda date: date.year)  # year from date
                case_data['KW'] = years_lst.apply(
                    lambda date: date.strftime("%V"))  # calendar week from date
                case_data['kalenderwoche_jahr'] = years_lst.apply(
                    lambda date: date.strftime("%G"))  # year from calendar week
            else:
                raise ValueError(f"case_data is missing column: {_col_name}!")

        def create_column_ersterZ():
            ersterZ = np.zeros(len(case_data))
            case_data['ersterZ'] = ersterZ
            for index, row in case_data.iterrows():
                if row['aufnahme_ts'] > row['triage_ts']:
                    row["ersterZ"] = 1

        for col in case_data.columns:
            if col.__contains__("_ts"):
                case_data[col] = case_data[col].apply(convert_time)

        create_columns_year_calweek_calweekyear()
        create_column_ersterZ()

    # create dataframe with case data from a case data file
    sets = get_all_result_sets()
    columns, data = read_data_all(sets)
    case_data = DataFrame(data=data, columns=columns).dropna()
    apply_conversions()

    return


if __name__ == "__main__":
    calc_los("cache/exports")
