from collections import defaultdict

def fill_header_info(ps, line):
    '''Fische interessante Angaben aus dem SMF Header
    '''
    tsplit = line.split('TIME = ')
    if len(tsplit) == 2:
        tsplit = tsplit[1].strip().split(' ', maxsplit=1)
        ps['TIME'] = tsplit[0]
        tsplit = tsplit[1].split('DATE = ', maxsplit=1)
        tsplit = tsplit[1].strip().split(' ', maxsplit=1)
        ps['DATE'] = tsplit[0]  
        tsplit = tsplit[1].split('SYSTEM-ID = ', maxsplit=1)
        tsplit = tsplit[1].strip().split(' ', maxsplit=1)
        ps['LPAR'] = tsplit[0]  

        
def fill_production_section(ps, line):
    '''Fische interessante Angaben aus der Product Section
    '''
    tsplit = line.split('JOB NAME = ')
    if len(tsplit) == 2:
        tsplit = tsplit[1].split(' ', maxsplit=1)
        ps['JOBNAME'] = tsplit[0]
        return
    tsplit= line.split('COMPRESSED DATA LENGTH = ')
    if len(tsplit) == 2:
        tsplit = tsplit[1].split(' ', maxsplit=1)
        ps['COMPLEN'] = int(tsplit[0])
        
rectypes = defaultdict(int)
header_infos = []
with open('x.txt', 'rb') as f:
    is_molsheader_or_footer = True
    is_smfrec_header = False
    is_smfrec_header = False
    is_product_section = False
    mols_header_or_footer = []
    for line in f:
        line = line.rstrip()
        if len(line) < 10:
            rectypes['SHORT'] += 1
            is_product_section = False
            continue

    #   if line.startswith(b'12345678901234...'):
        if line.startswith(b' '*14):
            rectypes['...LINE'] += 1
            continue

        if line.startswith(b'1****'):
            rectypes['SMFREC'] += 1
            is_molsheader_or_footer = False
            is_smfrec_header = True
            try:
                header_infos.append(header_info)
            except NameError:
                pass
            header_info = dict()
            continue

        if is_molsheader_or_footer:
            mols_header_or_footer.append(line.decode())
            continue

        if line.startswith(b' -----'):
            rectypes['TRANREC'] += 1
            is_molsheader_or_footer = False
            is_smfrec_header = False
            is_smfrec_header = False
            is_product_section = False
            continue

        if is_smfrec_header:
            if line.startswith(b' *  SMF PRODUCT SECTION'):
                rectypes['PRODSECTION'] += 1
                is_product_section = True
            if is_product_section:
                fill_production_section(header_info, line.decode())
            else:
                fill_header_info(header_info, line.decode())
            continue

        if line.startswith(b' '):
            rectypes['DATA'] += 1
        #   print(line)
            continue
        if line.startswith(b'1*** '):
            is_molsheader_or_footer = True
            continue


