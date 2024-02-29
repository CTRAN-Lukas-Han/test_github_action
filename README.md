# C-TRAN's Data Platform
## Setting up the development environment
First, it is recommended you set up a python virtual environment for the project.
In the folder you plan to use, run:
```
python3 -m venv .
```
Activate the virtual environment:
```
source bin/activate
```
## Cloning the repo and running the code
Clone the repository:
```
git clone git@github.com:C-TRANVancouver/Data_Platform.git
```

Change into the Data_Platform directory:
```
cd Data_Platform
```

run the install command:
```
pip install -e '.[dev]'
```

Assuming the build is successful, you can run:
```
dagster dev -m CTRAN
```
Note that "-m CTRAN" is needed for this development environment to load the CTRAN module, in production the module is reference with a workspace.yaml file as described in the dagster documentation.

You should see the project in the web browser at 127.0.0.1:3000 or whatever host and port was specified.

NOTE: before commiting any changes, be careful not to add the temp directory created by the dagster webserver for local storage e.g. 'tmp08fohn83', either shut down the webserver or commit the specific changes needed for your features.
