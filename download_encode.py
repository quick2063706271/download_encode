import os
import pandas as pd
import requests
import json
import argparse

ENCODE_BASE_URL = 'https://www.encodeproject.org'
# Force return from the server in JSON format
headers = {'accept': 'application/json'}


def parse_arguments():
    parser = argparse.ArgumentParser(prog='download ENCODE',
                                     description='Downloads ENCODE data from search result obtained from ENCODE restAPI')
    parser.add_argument('search_results_file',
                        metavar='F',
                        type=str,
                        help='Search results file obtained using restAPI')
    parser.add_argument("--filetypes", nargs='*', help="file types to download. example: bam bigwig fastq")
    parser.add_argument("--range", nargs=2, type=int, help='Download search result data from range[0] to range[1]')
    parser.add_argument("--directory", type=str, help="Save all files to this directory, default create and save at ./download/")
    args = parser.parse_args()
    # print(args.search_results_file)
    # print(args.filetypes)
    # print(args.range)
    # print(args.directory)
    return args


def download_encode_data(search_results_file, file_types, download_range=(0, None), download_directory="./download/"):
    # read first step search result json file
    f = open(search_results_file)
    search_results = json.load(f)
    f.close()

    if file_types is None:
        file_types = ["bam", "bigwig"]
    file_types = [file_type.lower() for file_type in file_types]

    # create saving path if path does not exist
    if download_directory[-1] != "/":
        download_directory = download_directory + "/"

    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
        print("create path")

    # download range
    range0 = download_range[0]
    range1 = download_range[1]

    if range1 is None or range1 > len(search_results["@graph"]):
        range1 = len(search_results["@graph"])
    if range0 >= range1:
        raise Exception("Invalid Range: first value is greater than second value")
    if range0 >= len(search_results["@graph"]):
        raise Exception("Invalid Range: first value is greater than search result length")
    if range0 < 0 or range1 < 0:
        raise Exception("Invalid Range: range value must not be negative")

    # Prepare dataframe for storing each file information
    df = pd.DataFrame(columns=['file_name', 'output_type', 'experiment_accession'])

    # Download file
    for i in range(range0, range1):
        search_result_i = search_results["@graph"][i]
        original_files = search_result_i["original_files"]
        for j in range(len(original_files)):
            file = original_files[j]
            url = ENCODE_BASE_URL + file + '?format=json'
            search_file = requests.get(url, headers=headers)
            search_file_json = search_file.json()
            # select file types we want
            if search_file_json["file_type"].lower() in file_types:
                accession_folder = download_directory + search_result_i['accession'] + "/"
                if not os.path.exists(accession_folder):
                    os.makedirs(accession_folder, exist_ok=True)
                    print("create path at {}".format(accession_folder))
                file_url = ENCODE_BASE_URL + search_file_json['href']
                file_output_name = accession_folder + search_file_json['href'].split("/")[-1]
                df = df.append({'file_name': file_output_name,
                                'output_type': search_file_json["output_type"],
                                'experiment_accession': search_result_i['accession']},
                               ignore_index=True)
                cmd_curl = 'curl -RL {} -o {}'.format(file_url, file_output_name)
                os.system(cmd_curl)
    df.to_csv(download_directory + 'download_file_info.csv')
    return


if __name__ == "__main__":
    # file_types = ["bam", "bigwig"]
    # download_encode_data('search_results.json',
    #                      file_types,
    #                      download_range=(0, 2),
    #                      download_directory="/dcl02/hongkai/data/mjiang/multitag/ENCODE/")
    args = parse_arguments()
    download_encode_data(search_results_file=args.search_results_file,
                         file_types=args.filetypes,
                         download_range=args.range,
                         download_directory=args.directory)
