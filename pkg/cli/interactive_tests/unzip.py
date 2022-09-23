import zipfile
import sys

z = zipfile.ZipFile(sys.argv[1])
z.extractall(path=sys.argv[2])
