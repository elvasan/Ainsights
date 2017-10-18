# Project AIDA Insights

AIDA Insights is an application providing customer insights into
a 'consumer journey' based on a set of given input identifiers
(e.g. LeadiD, email hash, phone hash, device id) submitted using a CSV
file format.

The inspiration for the structure of this project was taken from
[this article](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f)
and groups our code into different modules that can be submitted to spark
to be run as jobs.

## Local Development Setup

### Overview of Requirements

* Python Version: 3.4.3 (Default version available on Amazon EMR)
* Spark Version: 2.2.0 (Latest available)
* Java 8 (Spark Dependency)

### Java Installation

Ensure that you have Java 8 installed. You can do this by typing `java -version`.
If not, install via Homebrew:

    $ brew update
    $ brew cask install java8

Spark will pick up Java based on your JAVA_HOME variable so make sure you
have yours set correctly.

### Python Installation

The easiest way I've found to install different versions of Python is
to use pyenv. Since Amazon EMR supports Python 3.4.3 as of this writing,
we will be installing that version for local development using pyenv:

    $ brew update
    $ brew install pyenv
    $ pyenv install 3.4.3

This should install Python 3.4.3 to the `/Users/$username/.pyenv/versions/3.4.3`
directory.

### Create a Virtual Environment

**Create a virtual environment in the location of your choosing and install
the necessary requirements using pip. Below is an example of how this
could be done.**

Create a virtual environment using python3.4. If you installed 3.4 using
pyenv, you might need to copy the location when specifying Python 3.4. In
addition, you may need to upgrade pip (`pip install --upgrade pip`) before
running the requirements install if you have a version < 9.

    $ python3.4 -m venv ~/.venv/aida-insights
    $ source ~/.venv/aida-insights/bin/activate
    $ pip install -r requirements.txt

You can confirm that the main PySpark requirement was successfully installed
by typing `which pyspark` with your virtual environment activated and
reading the output to confirm it was installed to the virtual environment
bin directories.

### Project Structure

For this project we are following a structure based on a proposed best
practice outlined in [this article](https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f).
At the root level we have a `src` and `tests` directory. Under the `src`
directory we have `jobs` which contains all of the modules necessary to
run the application.

## Packaging and Running the Application Locally

When we submit the job to Spark we want to submit our main.py file as our
job file and the rest of the code as an extra dependency jobs.zip file. We
also want to include the paths to our input file and classifications file
as job arguments. This can be accomplished with the following actions:

    $ make build
    $ cd dist
    $ spark-submit --py-files jobs.zip main.py --job-args input_file_name=/tmp/test_mappings_3.csv output_dir=/tmp/output classification_file=/tmp/campaign_classifications.csv

The first line uses our accompanying Makefile to build and package our
files into a `dist/` directory. Within that directory we have our main.py file
which is responsible for launching our jobs. The jobs.zip file contains
all of the jobs that can be run by our main application that are currently under
our ./src/jobs directory. Next we activate our virtual environment that we
previously created. We then enter the dist directory and run our application.
We specify the spark-submit command that we pulled down with pip and we include
our jobs zip file as a parameter using the `--py-files` switch. We specify
the entry point into our application, `main.py` and the location of files we need
with the `---job-args` flag.

### Main.py arguments command

* `--job-args` Extra arguments to send to the PySpark job (example: --job-args foo=bar)

### AIDA Insights Job Arguments

There are currently two job arguments that are required for AIDA Insights to run
successfully. These are the locations of the input file and the classification file.
These can be absolute file paths on a server or they could even be s3 buckets
if the application is run within Amazon EMR.

* `input_file_name` The name and location of the input file given to us by the client
* `output_dir` The name and location of the output directory used by Spark
* `classification_file` The name and location of the classifications file used
to classify a lead.

Example:

    --job-args input_file_name=/tmp/test_mappings_3.csv output_dir=/tmp/output classification_file=/tmp/campaign_classifications.csv

## Intellij IDEA

### Setup

Import the code into Intellij from existing sources. For this project
we will use Intellij IDEA with the Python plugin. This plugin is managed
by the PyCharm team and has almost the same functionality as the
standalone PyCharm editor. If you need to install the Python plugin:

    Preferences... > Plugins

In the search bar type `Python` and select the official Python plugin.

Ensure that you are using the virtual environment Python version by
going to the `Project Structure` dialog, click `SDKs`, click the plus (`+`) sign,
then choose `Add local` and add the python 3.4 binary from the virtual environment
folder.

### Debugging the application using Intellij

Under the Run tab in Intellij select `Edit Configurations...`

In the Edit Configurations screen add a new Python configuration. Enter the following:

* In the script field enter the absolute path to the main file under the
src directory such as `~/git/aida-insights/src/main.py`.

* In the script parameters field enter the job arguments: `--job-args input_file_name=/tmp/input_file.csv output_dir=/tmp/output classification_file=/tmp/campaign_classifications.csv`

* Make sure the `Use specified interpreter` is selected and point to the
python instance you created in the virtual environment.

Using this setup you shouldn't have to do a `make build` first, you can
just set your debug points and hit the debug button.


## Unit Testing

### Running py.test from the command line

In order to run your PyTests from the command line you should be within
an activated Python virtual environment. You can run tests under any directory
or individual tests by specifying the path.

Directory level:

    $ python -m pytest /tests/_jobs/scoring

Individual file:

    $ python -m pytest /tests/_jobs/test_scoring.py

You can also run your pytests directly within Intellij. Right click
on the file you want to test and select `Run 'py.test in test_scoring'`.
This will initiate py.test within the Run window and you should see the
output of your test.


