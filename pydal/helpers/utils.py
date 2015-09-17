# -*- coding: utf-8 -*-

def read_file(filename, mode='r'):
    """Returns content from filename, making sure to close the file explicitly
    on exit.
    """
    f = open(filename, mode)
    try:
        return f.read()
    finally:
        f.close()
