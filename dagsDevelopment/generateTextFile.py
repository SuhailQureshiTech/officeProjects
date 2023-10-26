
from pathlib import Path

# replace with your preferred directory path
dir_path = Path('/var/sftp/searle')
file_name = "mydocument.txt"
file_path = dir_path.joinpath(file_name)

# check that directory exists
if dir_path.is_dir():
    with open(file_path, "w") as f:
        f.write("This text is written with Python.")
        print('File was created.')
else:
    print("Directory doesn\'t exist.")