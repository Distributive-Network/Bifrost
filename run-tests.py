import os
import sys
import subprocess

class bcolors:
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'


def run_test(test_file):
    process = subprocess.Popen(['python3', test_file], stdout = subprocess.PIPE, 
            stderr=subprocess.PIPE)
    try:
        outs, errs = process.communicate(timeout=max_timeout)
    except Exception as e:
        process.kill()
        outs, errs = process.communicate()
    print(outs)
    print(errs)
    return process.returncode




if __name__ == "__main__":
    import argparse

    cwd = os.path.realpath( os.path.dirname( __file__ ) )

    parser = argparse.ArgumentParser(description='A simple test runner.')
    parser.add_argument('--directory', type=str)
    args = parser.parse_args()

    for dirpath, dirnames, filenames in os.walk(args.directory):
        filenames = [ os.path.join( dirpath, filename) for filename in filenames ]
        for test_file in filenames:
            retcode = run_test(test_file, cwd=cwd)
            print(retcode)


