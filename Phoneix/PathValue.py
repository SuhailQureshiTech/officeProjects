import inspect
import sys
import os
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))


print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)
