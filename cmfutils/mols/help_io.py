'''Litter helpers to provide access to files
'''
import os
import fnmatch
def gen_pathes(generic_path):
    """Listdir mit pattern im Basename
    """
    in_dir = os.path.dirname(generic_path)
    in_dir = '.' if in_dir == '' else in_dir
    file_pattern = os.path.basename(generic_path)
    names = os.listdir(in_dir)
    names.sort()
    for name in fnmatch.filter(names, file_pattern):
        yield os.path.join(in_dir, name)

def gen_cat(pathlist):
    '''Generator: cat mit rstrip und decode utf-8
    '''
    for the_path in pathlist:
        print(f"Reading {the_path}")
        with open(the_path, encoding='ascii', errors='ignore') as opened:
            for rec in enumerate(opened):
                yield rec.rstrip() # ohne Whitespace newline am Ende

def genold_cat(pathlist):
    '''Generator: cat mit rstrip und decode utf-8
    '''
    i = 0
    for the_path in pathlist:
        print("Reading {:}".format(the_path))
        for rec in open(the_path, 'rb'):
            i = i + 1
            yield rec.rstrip().decode(errors='replace')
            # ohne Whitespace am Ende
    print("Recs: {:}".format(i))
