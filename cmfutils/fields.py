'''Building dictionary with descriptions of CMF-Fields
'''
import pkg_resources
import re
def read_fields():
    '''Locates fields.txt and yields field, description pairs from there
    '''
    this_package = __name__
    fields_path = '/'.join(('qw', 'fields.txt'))
    with pkg_resources.resource_stream(this_package, fields_path) as res:
        split_it = re.compile(' += *')
        for byte_line in res:
            line = byte_line.strip().decode()
            try:
                field, description = split_it.split(line, maxsplit=1)

                yield (field, description.strip("'"))
            except ValueError:
                pass
field_to_desc = dict(read_fields())
