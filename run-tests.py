import os
import sys
import subprocess

class bcolors:
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

#def run_test(test_file, cwd):
#    try:
#        retCode = subprocess.check_output(f"python3 {test_file}", shell=True, cwd = cwd, text=True)
#        return retCode
#    except subprocess.CalledProcessError as e:
#        return e.returncode

def run_test(test_file, cwd):
    try:
        p = subprocess.Popen(f"python3 {test_file}", shell=True, cwd=cwd, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        retcode = -1
        while True:
            retcode = p.poll()
            line = p.stdout.readline().decode('utf-8')
            print(f"python3 {os.path.basename(test_file)} --> {line}",end='')
            if retcode is not None:
                break
        return retcode
    except subprocess.CalledProcessError as cpe:
        return cpe.returncode



if __name__ == "__main__":
    import argparse

    cwd = os.path.realpath( os.path.dirname( __file__ ) )

    parser = argparse.ArgumentParser(description='A simple test runner.')
    parser.add_argument('--directory', type=str)
    args = parser.parse_args()

    test_dir = os.path.realpath( args.directory )

    files_to_test = []
    for dirpath, dirnames, filenames in os.walk(test_dir):
        if 'node_module' in dirpath:
            continue
        parsed_filenames = [ 
            os.path.realpath(os.path.join( cwd, dirpath, filename) )
            for filename in filenames
            if '.test.py' in filename
        ]
        files_to_test += parsed_filenames

    for i, test_file in enumerate(files_to_test[:1]):
        print(f'{i+1}/{len(files_to_test)} - Beginning test of {test_file}')
        retcode = run_test(test_file, cwd=test_dir)
        print(retcode)


