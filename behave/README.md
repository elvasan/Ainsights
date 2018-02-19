# Aida Insights test framework
### Overview
The AIDA Insights test framework uses behave/python for executing tests.

    - It uploads a client input file to S3 /incoming/{client}
    - Waits for the file to be moved to /app_data/{client}
    - Waits for the .zip file to be created in /outgoing/{client} by looking
      for a .zip file with a timestamp later than when the file was moved
    - Downloads the .zip file to a temp folder and extracts it
    - Compares the "actual" files in the .zip with stored "expected" files
        The expected files are manually validated versions of the output files.
    
### Configuration

    Configuration of each environment is handled in the config.json file located at the top level 
    of the behave directory. Each environment gets a separate section with the name matching one
    of the environments in your ~/.aws.config file.
    
    Example:
    
          "jornaya-dev": {
            "appName": "aida-insights",
            "region": "us-east-1"
        }
     
    The two properties needed are the application name and the AWS region name
    
### Execution

    Execute the test by running the command "behave -D env=jornaya-[dev|qa]"     
    
    Behave command line options:
    
        -D env=jornaya-[dev|qa] Defines the environment to run in (default qa)
        
        -D upload=yyyymmdd_HHMM Defines a UTC timestamp. When this is defined, the
                                upload and processing of a new file is skipped and
                                the first .zip file in /outgoing with a timestamp 
                                later than this is downloaded.  This is usefull for 
                                developing file tests without incurring the cost of
                                processing the file
     
### Adding additional clients / features

    Additional clients / features can be added by copying the beestest.feature file and 
    editing it as necessary. For example, the step that checks for valid labels (EARLY_JOURNEY, 
    LATE_JOURNEY, NOT_SEEN) does not need to be run for every client test.  
    
         
### Updating expected data files

    The tests are based on valid expected data files matched the input file submitted. 
    To update them, copy the following files from tmp/{client} => expected_data/{client}
        client_input_summary.csv                ==>  client_input_summary.csv
        client_output_summary.csv               ==>  client_output_summary.csv
        client_yyyy_mm_dd_hh_mm_ss_ffffff.csv   ==>  client_output_detail.csv  
        client_input_error.csv                  ==>  client_input_error.csv
