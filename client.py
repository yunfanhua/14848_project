import shlex
import subprocess
from google.cloud import storage
from os import listdir
from os.path import isfile, join

bucketName = 'dataproc-staging-us-west1-949914310960-nyxpajdc'
bucketInputFolder = 'InputFiles/'
bucketOutputFolder = 'output/'
localFolder = 'InputFiles/'

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucketName)


def upload_files():
    files = [f for f in listdir(localFolder) if isfile(join(localFolder, f))]
    for file in files:
        localFile = localFolder + file
        blob = bucket.blob(bucketInputFolder + file)
        blob.upload_from_filename(localFile)
    return f'Uploaded {files} to "{bucketName}" bucket.'


def list_uploaded_files():
    files = bucket.list_blobs(prefix=bucketInputFolder)
    fileList = [file.name for file in files if '.' in file.name]
    print(fileList)
    return fileList


def list_local_files():
    files = [f for f in listdir(localFolder) if isfile(join(localFolder, f))]
    print(files)


def delete_output_file():
    blobs = bucket.list_blobs(prefix=bucketOutputFolder)
    for blob in blobs:
        blob.delete()


def delete_uploaded_files():
    blobs = bucket.list_blobs(prefix=bucketInputFolder)
    for blob in blobs:
        blob.delete()


def submit_job(type, arg):
    delete_output_file()
    fileStr = str(list_uploaded_files())
    print('Spark job started for {}'.format(fileStr))
    fileStr = "".join(fileStr.split())
    subprocess.run(["gcloud", 'dataproc', 'jobs', 'submit', 'pyspark',
                    "gs://dataproc-staging-us-west1-949914310960-nyxpajdc/app.py",
                    "--cluster=cluster-6855",
                    "--region=us-west1",
                    "--", type, arg, fileStr], stdout=subprocess.PIPE)


def display_output():
    result = subprocess.run(["gsutil", "cat", "gs://dataproc-staging-us-west1-949914310960-nyxpajdc/output/*"],
                            capture_output=True)
    decoded_string = result.stdout.decode("unicode_escape")
    print(decoded_string)


options = {
    1: upload_files,
    2: list_local_files,
    3: list_uploaded_files,
    4: 'spark',
    5: delete_uploaded_files
}


def main():
    print("\n Welcome to Mini Search Engine")
    print("Please choose from the options below: \n")
    print('1. Upload files')
    print('2. List local files ready for upload')
    print('3. List uploaded files')
    print('4. Start Spark job')
    print('5. Delete uploaded files')
    print('6. exit')
    selection = int(input('Enter your selection...\n'))
    while selection != 6:
        while selection not in options:
            selection = int(input('Please enter a valid choice \n'))
        if selection == 4:
            type = input("Please enter 'search' for search keyword or 'top' for top N result\n")
            if type not in ['search', 'top']:
                print('Invalid option, please choose again')
                continue
            elif type == 'search':
                arg = input('Please enter the term you would like to search\n')
            else:
                arg = input('Please enter your N value\n')
            submit_job(type, arg)
            display_output()
        else:
            options[selection]()
        selection = int(input('Finished. Enter a new selection...\n'))
    print('goodbye')


if __name__ == "__main__":
    main()
