import sys
from pathlib import Path

from dynaconf import settings

from functions_s3 import functions_s3

def get_data_files():

    # INIT DICT TO STORAGE DATA FILES
    dict_data_files = {}

    # LOCAL DIR CONTAINS DATA FILES
    DIR_DATA_FILES = Path(settings.DIR_DATA_FILES).iterdir()

    # ITER OVER DATA FILES
    for data_file in DIR_DATA_FILES:
        data_file = data_file.absolute()
        filename = data_file.name

        if settings.get("DIR_BUCKET_S3"):
            dest_file = "{}/{}".format(settings.get("DIR_BUCKET_S3"), filename)
        else:
            dest_file = filename

        # STORING IN DICT
        dict_data_files[filename] = {"FILENAME": filename,
                                     "DATAFILE": data_file,
                                     "DESTINATION": dest_file}

    return dict_data_files


def upload_to_s3(dict_data_files, bucketname):

    # ITER OVER DATA FILES
    for data_file in dict_data_files:
        filename = dict_data_files[data_file]["DATAFILE"]
        bucketname = bucketname
        file_after_upload = dict_data_files[data_file]["DESTINATION"]

        # DOING THE UPLOAD
        functions_s3.upload_file_to_bucket_s3(filename=filename,
                                              bucketname=bucketname,
                                              file_after_upload=file_after_upload)


def orchestra_upload_data_files():

    # GETTING DATA FILES
    dict_data_files = get_data_files()

    # BUCKET S3 TO STORAGE DATA
    TARGET_BUCKET_S3 = settings.BUCKET_S3

    # UPLOAD TO S3 BUCKET
    validator = upload_to_s3(dict_data_files, TARGET_BUCKET_S3)

    return validator