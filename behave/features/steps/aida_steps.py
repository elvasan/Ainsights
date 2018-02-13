import zipfile
import os
import re
from behave import step
from datetime import datetime
import time
import pytz

## Global so it can be passed between scenarios
client_name = ""
client_input_dir = ""
client_expected_dir = ""
client_temp_dir = ""
client_timestamp = None
client_zip_file = ""

@step("I set up the test for client {}")
def step_setup_client_test(context, name):
    global client_name
    global client_temp_dir
    global client_input_dir
    global client_expected_dir

    # set up client and folder names
    client_name = name
    client_input_dir = os.path.join(context.inputDataDir, client_name)
    client_expected_dir = os.path.join(context.expectedDataDir, client_name)
    client_temp_dir = os.path.join(context.tempDataDir, client_name)

    # cleanup if needed before we start
    # this leaves last run's data availble for review
    if context.do_cleanup:
        if os.path.exists(client_temp_dir):
            # Remove all the files
            for file_name in os.listdir(client_temp_dir):
                path_name = os.path.join(client_temp_dir, file_name)

                try:
                    if os.path.isfile(path_name):
                        print("Cleanup: file "+file_name)
                        os.unlink(path_name)
                except Exception as e:
                    print(e)

            # Remove the directory
            print()
            try:
                os.rmdir(client_temp_dir)
            except Exception as e:
                print(e)

    # Create the directory if it does not exist
    if not os.path.exists(client_temp_dir):
        try:
            os.makedirs(client_temp_dir,0o755)
        except Exception as e:
            print(e)

@step('the environment is set up correctly')
def step_check_globals(context):
    # pylint: disable=W0613
    global client_name
    global client_input_dir
    global client_expected_dir
    global client_temp_dir

    assert client_name != ""
    assert client_input_dir != ""
    assert client_expected_dir != ""
    assert client_temp_dir != ""

@step('the input file is uploaded to the S3 incoming folder')
def step_upload_input_file(context):
    '''
        Upload the input request file to the "incoming" folder, simulating what
        happens after a client sends their file to the sftp server.  A later
        test should start by sending the file to sftp first.
        s3: jornaya-dev-us-east-1-aida-insights/incoming/{client}/{client.csv}
        :param context: the behave test context
        :param client_name:
        :return:
    '''
    global client_name
    global client_input_dir

    # context.uploadTimestamp can be set from the command line allowing
    # testing starting in the S3 outgoing/ folder without waiting for
    # the 20+ minute process to run on the uploaded data
    if context.uploadTimestamp is not None:
        print("context.uploadTimestamp set to {}, no upload will occur".format(context.uploadTimestamp))
    else:
        file_name = client_name + ".csv"
        local_file_path = os.path.join(client_input_dir, file_name)
        remote_file_path = "incoming/{}/{}".format(client_name,file_name)
        assert os.path.exists(local_file_path), "No input file found: " + local_file_path

        context.aws.upload_file(local_file_path, remote_file_path)

@step('the input file is processed by the system')
def step_check_file_processed(context):
    # pylint: disable=W0613
    '''
        The client input file is being "processed" when it is moved out of the
        "incoming" folder and into the app_data folder
        s3: jornaya-dev-us-east-1-aida-insights/incoming/{client}
        :param context: the behave test context
        :return:
    '''
    global client_name
    global client_timestamp

    # context.uploadTimestamp can be set from the command line allowing
    # testing starting in the S3 outgoing/ folder without waiting for
    # the 20+ minute process to run on the uploaded data
    if context.uploadTimestamp is not None:
        print("context.uploadTimestamp set to {}, using processed file".format(context.uploadTimestamp))
        client_timestamp = context.uploadTimestamp

    else:
        file_name = client_name + ".csv"
        remote_path = "incoming/{}/{}".format(client_name, file_name)

        limit_minutes = 5
        elapsed_minutes = 0

        print("")
        while elapsed_minutes <= limit_minutes:
            if not context.aws.check_file_or_direcory_present(remote_path):
                client_timestamp = pytz.utc.localize(datetime.utcnow())
                break

            print("Upload file still in /incoming, slept {}/{} minutes".format(elapsed_minutes,limit_minutes))
            time.sleep(60)
            elapsed_minutes += 1

        assert elapsed_minutes <= limit_minutes, "File {}.csv not moved from /incoming after {} minutes".format(
            client_name, limit_minutes)


@step('the output zip file exists on the system')
def step_check_output_zip_file_exists(context):
    '''
        The client output zip file exists when a file with a later creation date then
        the time the file was uploaded exists
        s3: "outgoing/{client}/{client}_yyyy_mm_dd_hh_mm_ss_ffffff.zip"
        :param context: the behave test context
        :return:
    '''
    global client_name
    global client_timestamp
    global client_zip_file

    limit_minutes   = 60
    elapsed_minutes = 0
    client_zip_file = None
    print("")
    while elapsed_minutes <= limit_minutes:
        response = context.aws.list_files_in_folder("outgoing/" + client_name)
        for file in response['Contents']:
            s3_file_name = file['Key']

            # 2005-11-17T07:13:48Z
            s3_file_timestamp = file['LastModified']
            if s3_file_timestamp > client_timestamp:
                client_zip_file = s3_file_name
                break

        if client_zip_file is not None:
            break

        print("No zip file in /outgoing newer than '{}', slept {}/{} minutes".format(client_timestamp,elapsed_minutes,limit_minutes))
        time.sleep(60)
        elapsed_minutes += 1

    assert client_zip_file is not None, "No {}.....zip found in /outgoing after {} minutes".format(
        client_name, limit_minutes)


@step('the zip file is downloaded')
def step_download_client_zip_file(context):
    '''
    Download the client zip file from AWS to the tmp folder
    and extract the files in it
    s3: "outgoing/{client}/{client}_yyyy_mm_dd_hh_mm_ss_ffffff.zip"
    :param context: the behave test context
    :return:
    '''
    global client_name
    global client_zip_file
    global client_temp_dir

    # Download
    # zip_name = os.path.basename(context.zipFileName)
    local_zip_file = os.path.join(client_temp_dir, os.path.basename(client_zip_file))
    context.aws.download_file(client_zip_file, local_zip_file)

    # Extract
    zf = zipfile.ZipFile(local_zip_file, "r")
    zf.extractall(client_temp_dir)
    zf.close()


@step('the zip file contains a client {} file')
def step_check_output_file_exists_in_s3(context, file_type):
    # pylint: disable=W0613
    '''
    Verify the file was extracted from the zip and exists in the tmp_dir
    :param context: the behave test context
    :param file_type: the file type (input summary, output summary, output detail)
    :return:
    '''
    global client_name
    global client_temp_dir

    file_name = get_actual_file_name(client_name, file_type, client_temp_dir)
    path_name = os.path.join(client_temp_dir, file_name)
    assert os.path.exists(path_name), "Zip file tmp/{}...zip does not contain a {} file".format(client_name,
                                                                                                path_name)


@step('the extracted client {} file matches the expected file')
def step_check_output_file_contents(context, file_type):
    # pylint: disable=W0613
    '''
    verify the file contents by comparing the file in the tmp_dir
    and the same file in the expected dir
    :param context: the behave test context
    :param file_type: the file type (input summary, output summary, output detail)
    :return:
    '''
    global client_name
    global client_temp_dir
    global client_expected_dir

    file_name_actual = get_actual_file_name(client_name, file_type, client_temp_dir )
    path_name_actual = os.path.join(client_temp_dir, file_name_actual)

    file_name_expected = get_expected_file_name(client_name, file_type)
    path_name_expected = os.path.join(client_expected_dir, file_name_expected)

    # Do the comparison
    files_match = True
    lines_read1 = 0
    fin1=open(path_name_expected,"r")
    fin2=open(path_name_actual,"r")
    line1 = fin1.readline()
    while line1:
        line2 = fin2.readline()
        lines_read1 += 1

        if line1!=line2:
            files_match = False
            print("line {}: expected: {}".format(lines_read1, line1))
            print("line {}: actual  : {}".format(lines_read1, line2))

        line1 = fin1.readline()

    fin1.close()
    fin2.close()

    # now make sure the actual file does not have more lines
    lines_read2 = get_file_len(path_name_actual)
    if lines_read1 != lines_read2:
        files_match = False
        print("File {}: expected {} lines".format(file_name_expected, lines_read1))
        print("File {}: actual   {} lines".format(file_name_actual, lines_read2))

    assert files_match, "Actual vs Expected, files did not match"


@step('the reported statuses are EARLY_JOURNEY, LATE_JOURNEY or NOT_SEEN')
def step_check_status_labels(context):
    # pylint: disable=W0613
    '''
    Story B-01629
    Test the returned statuses are one of EARLY_JOURNEY, LATE_JOURNEY or NOT_SEEN
    :param context:
    :return:
    '''
    global client_name
    global client_temp_dir
    global client_expected_dir

    file_name_actual = get_actual_file_name(client_name, "output detail", client_temp_dir )
    path_name_actual = os.path.join(client_temp_dir, file_name_actual)

    labels_match = True
    fin1=open(path_name_actual,"r")
    line1 = fin1.readline().rstrip()
    while line1:
        if not line1.startswith("recordid"):
            # split the line into a list
            # remove column 1
            # remove the expected values
            # if anything is leftf, print the record_id and the line that's left
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
    assert labels_match, "Unknown labels found in file "+file_name_actual


#    Helper function that returns the path name to the
#    file to be tested
def get_actual_file_name(cname, ftype, tempdir):
    '''
    Given an expected client and file type return the
    correct filename
    :param cname:
    :param ftype:
    :param tempdir:
    :return:
    '''
    file_name = None
    if ftype == 'output summary':
        file_name = cname + "_output_summary.csv"

    elif ftype == 'input summary':
        file_name = cname + "_input_summary.csv"

    elif ftype == 'output detail':
        # regex matches ^client_yyyy_mm_dd_hh_mm_ss_ffff.csv
        regex = re.compile("^" + cname + "_\d{4}_(\d{2}_){5}\d+\.csv")
        for file in os.listdir(tempdir):
            if regex.match(file):
                file_name = file
                break
    else:
        assert False, "Unknown file_type '{}' passed to step definition".format(ftype)

    return file_name


#   Helper function that returns the path name to the
#   expected file to be tested
def get_expected_file_name(cname, ftype):
    '''
    Given an expected client and file type return the
    correct filename for the expected client
    :param name:
    :param type:
    :return:

    '''
    file_name = None
    if ftype == 'output summary':
        file_name = cname + "_output_summary.csv"

    elif ftype == 'input summary':
        file_name = cname + "_input_summary.csv"

    elif ftype == 'output detail':
        file_name =  cname + "_output_detail.csv"
    else:
        assert False, "Unknown file_type '{}' passed to step definition".format(ftype)
        
    return file_name

#   helper function to count the lines in a file
def get_file_len(path_name):
    i = sum(1 for line in open(path_name, "r"))
    return i
