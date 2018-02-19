import os

class FileUtils:
    def delete_dir(dir_path):
        '''
        Clean up the temp data directory for this customer
        :param dir_path:
        :return:
        '''
        # cleanup if needed before we start
        # this leaves last run's data availble for review
    
        if os.path.exists(dir_path):
            # Remove all the files
            for file_name in os.listdir(dir_path):
                path_name = os.path.join(dir_path, file_name)
    
                try:
                    if os.path.isfile(path_name):
                        print("Cleanup: file " + file_name)
                        os.unlink(path_name)
                except Exception as e_info:
                    print(e_info)
    
            # Remove the directory
            try:
                os.rmdir(dir_path)
            except Exception as e_info:
                print(e_info)
    
    
    def create_dir(dir_path):
        '''
        Create the client data temp directory
        :return:
        '''
        # Create the directory if it does not exist
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path, 0o755)
            except Exception as e_info:
                print(e_info)
    
