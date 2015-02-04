'''
Created on 30 Aug 2012

@author: aburkimsher

1. turns LSF log (from past view tool) into CSV
2. turns CSV into a pytable H5 file

H5 file is useful because it allows very fast data filtering and querying
'''

import re
import csv
import time
import calendar
import tables


class text_to_csv_to_h5_transform_manager(object):
    #def __init__(self):
    #    self.cache_file_name = "./transform_cache.txt"
    #probably can't implement a cache because can't guarantee md5 to check
    #for consistency on different operating systems is available
       
    def transform(self, input_text_filename,
                  intermediary_csv_filename,
                  output_h5_filename, verbose = True):
        '''
        perform the actual transformation process
        detect if lsf 6 or lsf 7 input
        then
        lsf6 -> csv
        lsf7 -> csv
        csv -> h5
        '''
        
        #lsf6 or 7
        if self.test_if_lsf6(input_text_filename, verbose):
            if verbose:
                print "transforming lsf6 file into lsf7 csv file format"
            lsf7_format_filename = input_text_filename + "lsf7.txt"
            transform_lsf6_to_lsf7(input_text_filename,
                                   lsf7_format_filename)
            input_text_filename = lsf7_format_filename
        else:
            if verbose:
                print "transforming input lsf7 text file to csv format"
            transform_text_to_csv(input_text_filename,
                                  intermediary_csv_filename,
                                  verbose)
        
        #pytables h5
        if verbose:
            print "transforming csv file into pytables h5 format"
        #load up csv
        csv_contents_manager = file_contents_manager_iterator()
        csv_contents_manager.load_file(intermediary_csv_filename)
        headers = csv_contents_manager.get_headers()
        
        #init h5
        h5_file_manager = pytables_h5_file_manager(headers)
        h5_file_manager.h5_file_create_and_open(output_h5_filename)
        
        #write csv lines to h5
        for line in csv_contents_manager.get_contents()[1:]:
            h5_file_manager.h5_line_write(line)
        
        #and close
        h5_file_manager.h5_file_close()
        
        if verbose:
            print "h5 file transformation complete"

    def test_if_lsf6(self, input_text_filename, verbose = True):
        ''' should auto-detect whether it is an lsf6 or lsf7 file format
        lsf6 has lowercase headers
        lsf7 has uppercase headers '''
        newf = open(input_text_filename, 'r')
        topline = newf.next()
        first = topline.split(' ')
        if first[0].upper() == first[0]:
            if verbose:
                print "lsf7 format input file detected."
            return False
        else:
            if verbose:
                print "lsf6 format input file detected."
            return True


def transform_text_to_csv(infile_path, outfile_path, verbose = True):
    ''' to turn the LSF log file into a valid CSV:
    1. turn all comma-separated lists into semicolon-separated lists
    2. turn all space-separated fields into comma-separated fields
    '''
    inf = open(infile_path, 'r')
    outf = open(outfile_path, 'w')
    
    comma_matcher = re.compile(',')
    space_matcher = re.compile(' ')
    
    for line in inf:
        intermediate = comma_matcher.sub(';', line)
        outf.write(space_matcher.sub(',', intermediate))
    
    inf.close()
    outf.close()
    
    if verbose:
        print 'transform from txt to csv complete'




class file_contents_manager_iterator(object):
    '''
    reads through the csv file, separates the headers and the body
    the function names should be self-explanatory
    
    now optimised with an iterator.
    much quicker but careful - only allows one run through the file.
    much lower memory usage though - it was getting rather large    
    '''
    
    def __init__(self):
        self.contents = None
        self.headers = None
        self.loaded = False
        self.inputfile = None
     
    def load_file(self, filepath):
        if self.loaded:
            self.inputfile.close()
        self.contents = self.load_file_to_joblist(filepath)
        self.headers = self.title_indices(self.contents[0])
        self.loaded = True
        
    def load_file_to_joblist(self, filepath):
        self.inputfile = open(filepath)
        csvfile = csv.reader(self.inputfile)
        contents = [line for line in csvfile]
        return contents

    def title_indices(self, headerlinelist):
        titles = {}
        for i in range(len(headerlinelist)):
            titles[headerlinelist[i]] = i
        return titles
    
    def get_contents(self):
        if not self.loaded:
            print "file not loaded in get_contents"
            exit()
        else:
            return self.contents[1:]
    
    def get_headers(self):
        if not self.loaded:
            print "file not loaded in get_headers"
            exit()
        else:
            return self.headers
    
    def closing_bits(self):
        self.inputfile.close()
        self.loaded = False


def job_time_info_unixtime(jobline, header_titles):
    '''
    extract the submission, start and pending time stamps; number of cores
    all times transformed to unix time (seconds sinze 1970)
    to make the maths easier
    
    also calculates the execution time, pending time and response times
    in seconds as the intervals between the timestamps
    '''    
    submit_time = line_to_unixtime(jobline, 'SUBMIT_DATE', 'SUB_TIME', header_titles)
    start_time  = line_to_unixtime(jobline, 'START_DATE', 'START_TIME', header_titles)
    finish_time = line_to_unixtime(jobline, 'FINISH_DATE', 'FINISH_TIME', header_titles)
    
    cores = int(jobline[header_titles['CORES']])
    
    if finish_time == None:
        execution_time = 0
        response_time = 0
        core_time = 0
    else:
        execution_time = finish_time - start_time
        response_time = finish_time - submit_time
        core_time = execution_time * cores
    
    if start_time == None:
        pending_time = 0
    else:
        pending_time = start_time  - submit_time
    
    timedict = {
                'submit_time'    : submit_time,
                'start_time'     : start_time,
                'finish_time'    : finish_time,
                'execution_time' : execution_time,
                'response_time'  : response_time,
                'pending_time'   : pending_time,
                'cores'          : cores,
                'core_seconds'   : core_time
               }
    return timedict



def line_to_unixtime(line, date_header, time_header, header_titles):
    ''' convert text string to unix time '''
    t = header_titles
    if (line[t[date_header]] == 'NONE') or (line[t[time_header]] == 'NONE'):
        return 0
    else:
        time_format_string = "%d-%m-%Y %H:%M:%S" #MAGIC STRING!! for time format of input text file
        time_string = line[t[date_header]] + " " + line[t[time_header]]
        python_time = time.strptime(time_string , time_format_string)
        unix_time = calendar.timegm(python_time)
        return unix_time


def transform_lsf6_to_lsf7(infile_path, outfile_path):
    """
    converter for the output of the Superdome logs (LSF 6) into the format of the main Grid logs (LSF 7)
    Andrew Burkimsher
    June 2012
    
    turns this format:
    
    jobid    event_time    user    lsfgroup    subdomain    domain    first_name    last_name    site    company    jobname    queue    cluster    site    submit_year    submit_month    submit_day    submit_hour    submit_min    start_year    start_month    start_day    start_hour    start_min    finish_year    finish_month    finish_day    finish_hour    finish_min    exec_hosts    job_status    project    app    num_exec_hosts    runtime    categ_runtime    pendtime    pend_hours    core_hours    rack_hours    submit_dir    exit_status    max_res_mem    max_virtual_mem    error_code
    super_54067_2012_6_19    1340118427    abfxla2            other                    DAMICAD    weekend    other        2012    6    19    14    35    2012    6    19    14    39    2012    6    19    15    7    ['1*sn41168b']    64    DAMICAD    unknown_serial    1    1683    b_lt_12hours    243    0.0675    0.47    0.01    /i/caf/structures/projects/a320_neo/r27/may8/08a_cleat    0    98    101    OK
    super_51214_2012_6_18    1340073253    abf95628    ug_aerowing    aerodesign    aerodomain    Ben    COMMIS    FP_UK    AIRBUS    A333_6P8B_T700v2_EGA_ADV_MM84_a1p585    egaserial    other    FP_UK    2012    6    18    23    42    2012    6    18    23    46    2012    6    19    2    34    ['1*eguk1050@airbus02@airbus02']    64    a330    unknown_serial    1    10070    b_lt_12hours    243    0.0675    2.8    0.09    /i/caf/aerousers/proj5/ns/a330/vol2/A333_6P8B_T700v2_EGA_ADV/Re41p1m_Sm_Extr/M84    0    20362    20388    OK
    
    
    into this format:
    
    CJOBID    USER    STATUS    CLUSTER    QUEUE    MASTER_NODE    LSF_GROUP    SUBMIT_DATE    SUB_TIME    START_DATE    START_TIME    FINISH_DATE    FINISH_TIME    APP    CORES    RUNH    CPUD    PROJECT    JOBNAME    PENDH    PENDS    ERROR_CODE    CORRECTED
    521973    to102882    FINISH    ham_a    loc_all_par    hcn0154    ug_aerodat2    06/06/2012    10:08:00    06/06/2012    11:01:00    06/06/2012    12:58:00    elsa    24    1    1    RT_EGA_2DA-HS-ELSA-UNSTEADY-OPTIMOP    239164_1_ELSA    1    3196    OK    NONE
    376691    st38968    FINISH    ham_a    loc_all_par    hcn0540    ug_aerotsim    20/02/2012    19:40:00    20/02/2012    22:12:00    21/02/2012    01:59:00    tau_flow_short    2    3    0    RT_EGA_1ALTRAN    181120_1_TAU    3    9130    OK    NONE
    
    
    """
    
    inf = open(infile_path, 'r')
    outf = open(outfile_path, 'w')
    
    #sort out the headers for the target file
    topline_ex = "CJOBID,USER,STATUS,CLUSTER,QUEUE,MASTER_NODE,LSF_GROUP,SUBMIT_DATE,SUB_TIME,START_DATE,START_TIME,FINISH_DATE,FINISH_TIME,APP,CORES,RUNH,CPUD,PROJECT,JOBNAME,PENDH,PENDS,ERROR_CODE,CORRECTED "
    outf.write(topline_ex) 
    headers = topline_ex[:-1].split(',')   
    head_order_dict = {}   
    for i in range(len(headers)):
        head_order_dict[headers[i]] = i    
    topline_in = inf.next()
    headers_in = topline_in[:-1].split(',')
    in_head_order_dict = {}
    for i in range(len(headers_in)):
        in_head_order_dict[headers_in[i]] = i
    
    #sort out problems with exec host lists with commas in a csv file.    
    lsi = lambda x: lsplit[in_head_order_dict[x]]
    
    for line in inf:
        lsplit = line[:-1].split(',')        
        if len(lsplit) != len(headers_in):            
            if lsi('exec_hosts')[0] == '"':
                exec_host_counter = 1
                while lsplit[in_head_order_dict['exec_hosts'] + exec_host_counter][-1] != '"':
                    exec_host_counter += 1                                
                for i in range(exec_host_counter):
                    lsplit[in_head_order_dict['exec_hosts']] = (
                        lsplit[in_head_order_dict['exec_hosts']] + 
                        lsplit.pop(in_head_order_dict['exec_hosts'] + 1))               
            elif len(lsplit) != len(headers_in):                
                print "error, line lengths still don't match after correction"
                print topline_in
                print line
                exit()
        
        #create the output dictionary for each line
        od = {}
        
        od["CJOBID"] = lsi('jobid').split('_')[1]
        od["USER"] = lsi('user')
        od["STATUS"] = lsi('job_status')
        od["CLUSTER"] = lsi('cluster')
        od["QUEUE"] = lsi('queue')
        od["MASTER_NODE"] = lsi('site')
        od["LSF_GROUP"] = lsi('lsfgroup')
        
        od["SUBMIT_DATE"] = lsi('submit_day') + '-' + lsi('submit_month') + '-' + lsi('submit_year')
        od["SUB_TIME"] = lsi('submit_hour') + ':' + lsi('submit_min') + ':00'
        od["START_DATE"] =lsi('start_day') + '-' + lsi('start_month') + '-' + lsi('start_year')
        od["START_TIME"] = lsi('start_hour') + ':' + lsi('start_min') + ':00'
        od["FINISH_DATE"] = lsi('finish_day') + '-' + lsi('finish_month') + '-' + lsi('finish_year')
        od["FINISH_TIME"] = lsi('finish_hour') + ':' + lsi('finish_min') + ':00'
        
        od["APP"] = lsi('app')
        od["CORES"] = lsi('num_exec_hosts')
        od["RUNH"] = str(float(lsi('runtime')) / (60.0 * 60.0))
        od["CPUD"] = str(float(lsi('core_hours')) / 24.0)
        od["PROJECT"] = lsi('project')
        od["JOBNAME"] = lsi('jobname')
        od["PENDH"] = lsi('pend_hours')
        od["PENDS"] = lsi('pendtime')
        od["ERROR_CODE"] = lsi('error_code')
        od["CORRECTED"] = 'NONE'
        
        c = re.compile(',')
        
        outbits_array = [c.sub(';',od[h]) for h in headers]
        out_line = ','.join(outbits_array) + '\n'
        
        #write the output line
        outf.write(out_line)
     
    #close the files
    inf.close()
    outf.close()


class jobrecord(tables.IsDescription):
    ''' defines the content fields of the pytable
    pylint complains a lot about this, but that is because it is valid
    pytables syntax, rather than python syntax '''
    CJOBID          = tables.Int64Col()   # 32-bit Unsigned integer
    USER            = tables.StringCol(64) # 64-character String
    STATUS          = tables.StringCol(64) # 64-character String
    CLUSTER         = tables.StringCol(64) # 64-character String
    QUEUE           = tables.StringCol(64) # 64-character String
    MASTER_NODE     = tables.StringCol(64) # 64-character String
    LSF_GROUP       = tables.StringCol(64) # 64-character String
    APP             = tables.StringCol(64) # 64-character String
    PROJECT         = tables.StringCol(256)# 256-character String
    JOBNAME         = tables.StringCol(256)# 256-character String
    ERROR_CODE      = tables.StringCol(64) # 64-character String
    SUBMIT_DATETIME = tables.Int64Col()   # 64-bit Unsigned integer
    START_DATETIME  = tables.Int64Col()   # 64-bit Unsigned integer
    FINISH_DATETIME = tables.Int64Col()   # 64-bit Unsigned integer
    EXECUTION_TIME  = tables.Int64Col()   # 64-bit Unsigned integer
    RESPONSE_TIME   = tables.Int64Col()   # 64-bit Unsigned integer
    PENDING_TIME    = tables.Int64Col()   # 64-bit Unsigned integer
    CORE_SECONDS    = tables.Int64Col()   # 64-bit Unsigned integer
    CORES           = tables.Int64Col()   # 32-bit Unsigned integer




class pytables_h5_file_manager():
    ''' wraps the pytables commands for dealing with h5 files'''
    def __init__(self, headers):
        self.file_created = False
        self.new_row_handle = None
        self.h5_file_handle = None
        self.table_handle = None
        self.headers = headers
    
    def h5_file_create_and_open(self, new_file_name):
        # Open a file in "w"rite mode
        self.h5_file_handle = tables.openFile(new_file_name,
                                              mode = "w",
                                              title = "Jobs Data File")
        # Create a new group under "/" (root)
        group = self.h5_file_handle.createGroup("/",
                                                'jobs',
                                                'Job Information')
        # Create one table on it
        table = self.h5_file_handle.createTable(group,
                                                'extract',
                                                jobrecord,
                                                "extract example")
        
        self.table_handle = table
        self.new_row_handle = table.row
        self.file_created = True
        
    
    
    def h5_line_write(self, csv_file_line):
        if self.file_created:
            self.csv_file_line_to_jobrecord(csv_file_line,
                                            self.new_row_handle,
                                            self.headers)
            self.new_row_handle.append()
        else:
            print "error, h5 file not yet created or opened"
            exit()
    
    def h5_file_close(self):
        if self.file_created:
            self.table_handle.flush()
            self.h5_file_handle.close()
        else:
            print "error, h5 file not yet created or opened"
            exit()
        
        
    def csv_file_line_to_jobrecord(self, line, new_row, headers):
        ''' create a new record in the pytable for each line of input '''
        
        time_info = job_time_info_unixtime(line, headers)
        
        new_row['CJOBID']       = int(line[headers['CJOBID']])
        new_row['USER']         = line[headers['USER']]                     
        new_row['STATUS']       = line[headers['STATUS']]                     
        new_row['CLUSTER']      = line[headers['CLUSTER']]                     
        new_row['QUEUE']        = line[headers['QUEUE']]                     
        new_row['MASTER_NODE']  = line[headers['MASTER_NODE']]                     
        new_row['LSF_GROUP']    = line[headers['LSF_GROUP']]                     
        new_row['APP']          = line[headers['APP']]                     
        new_row['PROJECT']      = line[headers['PROJECT']]                     
        new_row['JOBNAME']      = line[headers['JOBNAME']]                     
        new_row['ERROR_CODE']   = line[headers['ERROR_CODE']]   
        
        new_row['SUBMIT_DATETIME']  = time_info['submit_time']
        new_row['START_DATETIME']   = time_info['start_time']
        new_row['FINISH_DATETIME']  = time_info['finish_time']
        new_row['EXECUTION_TIME']   = time_info['execution_time']
        new_row['RESPONSE_TIME']    = time_info['response_time']
        new_row['PENDING_TIME']     = time_info['pending_time']
        new_row['CORE_SECONDS']     = time_info['core_seconds']
        new_row['CORES']            = time_info['cores'] 





if __name__ == "__main__":
    text_file = "./test/super_analysis.csv"
    inter_csv_file = "./test/jobs4_new.csv"
    out_table_file = "./test/jobs4_table_new.h5"
    
    transformer = text_to_csv_to_h5_transform_manager()
    
    transformer.transform(text_file,
                          inter_csv_file,
                          out_table_file)



