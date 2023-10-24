import os

from pandas import DataFrame


def calc_los(broker_export_path):
    # returns a list of filenames that contain results from a export
    def get_all_result_sets():
        exports_url = 'cache/exports'
        result_sets = [f for f in os.listdir(exports_url) if f.__contains__("case_data")]
        return result_sets

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

    # create dataframe with case data from a case data file
    sets = get_all_result_sets()
    columns, data = read_data_all(sets)
    case_data = DataFrame(data=data, columns=columns)

    return


if __name__ == "__main__":
    calc_los("cache/exports")
