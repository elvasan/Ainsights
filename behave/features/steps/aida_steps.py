import zipfile
import os
import re
import time
from datetime import datetime
from behave import step
from utils.file_utils import FileUtils
import pytz

# Global so it can be passed between scenarios
CLIENT_NAME = ""
CLIENT_INPUT_FILE = ""
CLIENT_EXPECTED_DIR = ""
CLIENT_TEMP_DIR = ""
CLIENT_TIMESTAMP = None
CLIENT_ZIP_FILE = ""


@step("I use client {}")
def step_setup_client_name(context, c_name):
    '''
    Set up the AIDA Insights client name for the test
    :param context: The test context
    :param c_name:  The client_name to use
    :return:
    '''
    global CLIENT_NAME
    CLIENT_NAME = c_name


@step('input file {}')
def setup_setup_input_file(context, file_name):
    '''
    Set the input file to {input_data_dir}/{client}/{filename}
    This allows us to keep all input files together fo a given client
    :param context:   The test context
    :param file_name: The input filename to upload
    :return:
    '''
    global CLIENT_NAME
    global CLIENT_INPUT_FILE
    CLIENT_INPUT_FILE = os.path.join(context.inputDataDir, CLIENT_NAME, file_name)
    if not os.path.exists(CLIENT_INPUT_FILE):
        print("Input file " + CLIENT_INPUT_FILE + " not found")
        raise SystemExit(1)


@step('the test environment is set up')
def step_setup_environment(context):
    '''
    Set up the test environment
    :param context: The test context
    :return:
    '''
    global CLIENT_NAME
    global CLIENT_INPUT_FILE
    global CLIENT_TEMP_DIR
    global CLIENT_EXPECTED_DIR

    folder = os.path.splitext(os.path.basename(CLIENT_INPUT_FILE))[0]
    CLIENT_EXPECTED_DIR = os.path.join(context.expectedDataDir, folder)
    if not os.path.exists(CLIENT_EXPECTED_DIR):
        print("Expected data directory " + CLIENT_EXPECTED_DIR + " not found")
        raise SystemExit(1)

    #
    # Set up the temp folder to match the expected folder name.
    # This lets us save multiple feature file runs
    #
    CLIENT_TEMP_DIR = os.path.join(context.tempDataDir, folder)
    if context.do_cleanup:
        FileUtils.delete_dir(CLIENT_TEMP_DIR)

    FileUtils.create_dir(CLIENT_TEMP_DIR)


@step('the input file is uploaded to the S3 incoming folder')
def step_upload_input_file(context):
    '''
        Upload the CLIENT_INPUT_FILE to the "incoming" folder, simulating what
        happens after a client sends their file to the sftp server.  A later
        test should start by sending the file to sftp first.
        local: input_data/{client}/CLIENT_INPUT_FILE
        s3:    jornaya-dev-us-east-1-aida-insights/incoming/{client}/{client.csv}
        :param context: the behave test context
        :return:
    '''
    global CLIENT_NAME
    global CLIENT_INPUT_FILE

    # context.uploadTimestamp can be set from the command line allowing
    # testing starting in the S3 outgoing/ folder without waiting for
    # the 20+ minute process to run on the uploaded data
    if context.uploadTimestamp is not None:
        print("context.uploadTimestamp set to {}, no upload will occur".format(context.uploadTimestamp))
    else:
        remote_file_path = "incoming/{}/{}".format(CLIENT_NAME, CLIENT_NAME + ".csv")

        context.aws.upload_file(CLIENT_INPUT_FILE, remote_file_path)


@step('the input file is processed by the system')
def step_check_file_processed(context):
    '''
        The client input file is being "processed" when it is moved out of the
        "incoming" folder and into the app_data folder
        s3: jornaya-dev-us-east-1-aida-insights/incoming/{client}
        :param context: the behave test context
        :return:
    '''
    global CLIENT_NAME
    global CLIENT_TIMESTAMP

    # context.uploadTimestamp can be set from the command line allowing
    # testing starting in the S3 outgoing/ folder without waiting for
    # the 20+ minute process to run on the uploaded data
    if context.uploadTimestamp is not None:
        print("context.uploadTimestamp set to {}, using processed file".format(context.uploadTimestamp))
        CLIENT_TIMESTAMP = context.uploadTimestamp

    else:
        remote_file_path = "incoming/{}/{}".format(CLIENT_NAME, CLIENT_NAME + ".csv")
        limit_minutes = 5
        elapsed_minutes = 0

        while elapsed_minutes <= limit_minutes:
            if not context.aws.check_file_or_direcory_present(remote_file_path):
                CLIENT_TIMESTAMP = pytz.utc.localize(datetime.utcnow())
                break

            print("Upload file still in /incoming, slept {}/{} minutes".format(elapsed_minutes, limit_minutes))
            time.sleep(60)
            elapsed_minutes += 1

        assert elapsed_minutes <= limit_minutes, "File {}.csv not moved from /incoming after {} minutes".format(
            CLIENT_NAME, limit_minutes)


@step('the output zip file exists on the system')
def step_check_output_zip_file_exists(context):
    # pylint: disable=W0603
    '''
        The client output zip file exists when a file with a later creation date then
        the time the file was uploaded exists
        s3: outgoing/{client}/{client}_yyyy_mm_dd_hh_mm_ss_ffffff.zip
        :param context: the behave test context
        :return:
    '''
    global CLIENT_NAME
    global CLIENT_TIMESTAMP
    global CLIENT_ZIP_FILE

    limit_minutes = 60
    elapsed_minutes = 0
    CLIENT_ZIP_FILE = None

    while elapsed_minutes <= limit_minutes:
        response = context.aws.list_files_in_folder("outgoing/" + CLIENT_NAME)
        for file_info in response['Contents']:
            remote_file_path = file_info['Key']

            # 2005-11-17T07:13:48Z
            remote_file_timestamp = file_info['LastModified']
            if remote_file_timestamp > CLIENT_TIMESTAMP:
                CLIENT_ZIP_FILE = remote_file_path
                break

        if CLIENT_ZIP_FILE is not None:
            break

        print("No zip file in /outgoing newer than '{}', slept {}/{} minutes".format(CLIENT_TIMESTAMP, elapsed_minutes,
                                                                                     limit_minutes))
        time.sleep(60)
        elapsed_minutes += 1

    assert CLIENT_ZIP_FILE is not None, "No {}.....zip found in /outgoing after {} minutes".format(
        CLIENT_NAME, limit_minutes)


@step('the zip file is downloaded')
def step_download_client_zip_file(context):
    '''
    Download the client zip file from AWS to the tmp folder
    and extract the files in it
    s3   : outgoing/{client}/{client}_yyyy_mm_dd_hh_mm_ss_ffffff.zip
    local: data/tmp/{input_file}/{client}_yyyy_mm_dd_hh_mm_ss_ffffff.zip
    :param context: the behave test context
    :return:
    '''
    global CLIENT_NAME
    global CLIENT_ZIP_FILE
    global CLIENT_TEMP_DIR

    # Download
    # zip_name = os.path.basename(context.zipFileName)
    local_file_path = os.path.join(CLIENT_TEMP_DIR, os.path.basename(CLIENT_ZIP_FILE))
    context.aws.download_file(CLIENT_ZIP_FILE, local_file_path)

    # Extract
    zip_file = zipfile.ZipFile(local_file_path, "r")
    zip_file.extractall(CLIENT_TEMP_DIR)
    zip_file.close()


@step('the extracted client {} file matches the expected file')
def step_check_output_file_contents(context, file_type):
    # pylint: disable=W0613
    '''
    verify the file contents by comparing the file in CLIENT_TEMP_DIR
    and the same file in the expected dir
    :param context: the behave test context
    :param file_type: the file type (input summary, output summary, output detail)
    :return:
    '''
    global CLIENT_NAME
    global CLIENT_TEMP_DIR
    global CLIENT_EXPECTED_DIR

    found = True
    local_path_actual = get_local_path_actual(CLIENT_NAME, file_type, CLIENT_TEMP_DIR)
    if local_path_actual is None:
        print("No actual file found: " + local_path_actual)
        found = False

    local_path_expected = get_local_path_expected(CLIENT_NAME, file_type,CLIENT_EXPECTED_DIR)
    if local_path_expected is None:
        print("No expected file found: " + local_path_expected)
        found = False

    assert found, "One or more needed files does not exist"

    # Do the comparison
    files_match = True
    lines_read1 = 0
    fin1 = open(local_path_expected, "r")
    fin2 = open(local_path_actual, "r")
    line1 = fin1.readline()
    while line1:
        line2 = fin2.readline()
        lines_read1 += 1

        if line1 != line2:
            files_match = False
            print("line {}: expected: {}".format(lines_read1, line1))
            print("line {}: actual  : {}".format(lines_read1, line2))

        line1 = fin1.readline()

    fin1.close()
    fin2.close()

    # now make sure the actual file does not have more lines
    lines_read2 = get_file_length(local_path_actual)
    if lines_read1 != lines_read2:
        files_match = False
        print("File {}: expected {} lines".format(file_name_expected, lines_read1))
        print("File {}: actual   {} lines".format(file_name_actual, lines_read2))

    assert files_match, "Actual vs Expected, files did not match"


@step('the reported statuses are EARLY_JOURNEY, LATE_JOURNEY or NOT_SEEN')
def step_check_status_labels(context):
    '''
    Story B-01629
    Test the returned statuses are one of EARLY_JOURNEY, LATE_JOURNEY or NOT_SEEN
    :param context:
    :return:
    '''
    global CLIENT_NAME
    global CLIENT_TEMP_DIR
    global CLIENT_EXPECTED_DIR

    found = True
    local_path_actual = get_local_path_actual(CLIENT_NAME, "output detail", CLIENT_TEMP_DIR)
    if local_path_actual is None:
        print("No actual file found: " + local_path_actual)
        found = False

    assert found, "One or more needed files does not exist"

    labels_match = True
    fin1 = open(local_path_actual, "r")
    line1 = fin1.readline().rstrip()
    while line1:
        if not line1.startswith("recordid"):
            # split the line into a list
            # remove column 1
            # remove the expected values
            # if anything is left, print the record_id and the line that's left
            entries = line1.split(",")
            record_id = entries.pop(0)

            entries = [x for x in entries if x != 'EARLY_JOURNEY']
            entries = [x for x in entries if x != 'LATE_JOURNEY']
            entries = [x for x in entries if x != 'NOT_SEEN']

            if entries:
                labels_match = False
                print("Unknown status. record_id: {}, line: {}".format(record_id, entries))
        line1 = fin1.readline().rstrip()

    fin1.close()
    assert labels_match, "Unknown labels found in file " + file_name_actual


@step('the {} file does not exist in the zip file')
def step_file_does_not_exists(context, file_type):
    '''
    Verify the {client}_input_summary, {client}_output_summary and {client)_yyyy_mm_dd_hh_mm_ss_ffff
    CSV files have not been created (ie they are not in the temp folder)
    :param context: 
    :return:
    '''
    global CLIENT_NAME
    global CLIENT_TEMP_DIR

    local_path_actual = get_local_path_actual(CLIENT_NAME, file_type, CLIENT_TEMP_DIR)
    assert local_path_actual is None, "Client {} file was found when it was not expected".format(file_type)


def get_local_path_actual(c_name, file_type, temp_dir):
    '''
    Helper function.
    Given a client and file type return the actual file's pathname
    or None if the file does not exist
    :param c_name:   The client name
    :param file_type:   The file type (input summary, output summary, output detail, input_error)
    :param temp_dir: The directory under data/tmp the file will be in
    :return:         The full path name to the file or None if it does not exist
    '''
    path_name = None
    if file_type == 'output summary':
        path_name = os.path.join(temp_dir, c_name + "_output_summary.csv")

    elif file_type == 'input summary':
        path_name = os.path.join(temp_dir, c_name + "_input_summary.csv")

    elif file_type == 'input error':
        path_name = os.path.join(temp_dir, c_name + "_input_error.csv")

    elif file_type == 'output detail':
        # regex matches ^client_yyyy_mm_dd_hh_mm_ss_ffff.csv
        regex = re.compile("^" + c_name + "_\d{4}_(\d{2}_){5}\d+\.csv")
        for f_info in os.listdir(temp_dir):
            if regex.match(f_info):
                path_name = os.path.join(temp_dir, f_info)
                break
    else:
        assert False, "Unknown file_type '{}' passed to step definition".format(file_type)

    if os.path.exists(path_name):
        return path_name

    return None


def get_local_path_expected(c_name, file_type, expected_dir):
    '''
    Helper function
    Given a client and file type return the expected file's pathname
    or None if the file does not exist
    :param c_name:   The client name
    :param file_type:   The file type (input summary, output summary, output detail, input_error)
    :param temp_dir: The directory under data/expected_data the file will be in
    :return:         The full path name to the file or None if it does not exist
    '''
    file_name = None
    if file_type == 'output summary':
        path_name = os.path.join(expected_dir, c_name + "_output_summary.csv")

    elif file_type == 'input summary':
        path_name = os.path.join(expected_dir, c_name + "_input_summary.csv")

    elif file_type == 'output detail':
        path_name = os.path.join(expected_dir, c_name + "_output_detail.csv")

    elif file_type == 'input error':
        path_name = os.path.join(expected_dir, c_name + "_input_error.csv")
    else:
        assert False, "Unknown file_type '{}' passed to step definition".format(file_type)

    if os.path.exists(path_name):
        return path_name

    return None

#   helper function to count the lines in a file
def get_file_length(local_path_name):
    '''
    Helper function
    Counts the number of lines in a file
    :param path_name: The pathname of the file
    :return: The number of lines in the file
    '''
    return sum(1 for line in open(local_path_name, "r"))
