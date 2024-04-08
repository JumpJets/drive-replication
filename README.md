# Drive replication

An utility designed to copy data to another drive. A typical scenario would be buying new HDD/SSD and replacing old one, however all data would need to be replicated to the new drive.

As an optional scenario it does not limited to replicate straight from drive root to another drive root, it also allow you to copy files within subfolders even within one drive.

Most of the code target NTFS drives within Windows OS, however it does not limit to that and should be cross-platform at least on Linux OS _(if there is a bug, feel free to open an issue)_

At this time the following information is preserved on replication:

-   File content
-   File `stats()` (this include attributes like creation date, modify date, etc.)
-   Windows (NTFS) attributes for files and folders:
    -   Readonly
    -   Hidden
        -   Linux use `.` as prefix for hidden files and folders
    -   Archive
    -   System
    -   ~~Compressed~~
    -   ~~Encrypted~~
-   Hard links (files)
-   Symbolic links (files and folders)
-   Windows: Junction points _(NOTE: an implementation using Windows built-in command `mklink` as other external library implementations with DLL call, etc have bugs and not maintained)_
-   Windows: Certain files are deliberately ignored, such as `pagefile.sys` as it used to store RAM on drive or `System Volume Information` as it filled automatically by OS and have permission restrictions.

# Requirements

Since `Path.walk()` only added in **Python 3.12**, this is a minimal version required to work for this script.

There are 3 external library, you can install them with
```sh
pip install -r requirements.txt
```

# Run

You can run script from anywhere without any arguments and you will be prompted to enter source directory, destination directory and optional excluded paths (with wildcard support).

Or you can run script with those arguments like:
```sh
python ./main.py "path/to/source" "path/to/destination"
python ./main.py "path/to/source" "path/to/destination" "optional/ignore/1" "optional/ignore/2"
```

For the Windows a typical example would be:
```sh
python ./main.py C:\ D:\
python ./main.py D:\media E:\media
python ./main.py D:\media E:\media D:\media\exclude\
```
