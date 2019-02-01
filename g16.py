#! /usr/bin/env python3.5
# -*- coding: utf-8 -*-

# This file allows to submit Gaussian 16 jobs to the kirk cluster
# File: g16.py
# Author: Sergi Pérez Labernia, 2019.

# Imports
import os
import sys
from argparse import ArgumentParser
from pathlib import Path

# Global definitions
filename = 'script.pbs'
program = 'gaussian'
secondsBorg1 = 10800000
secondsBorg2 = 21600000
secondsBorg3 = None
secondsBorgTest = 129600
user = os.getlogin()
hostname = ''

# Arguments
parser = ArgumentParser(description='g16.py allows to summit Gaussian16 jobs to the kirk cluster.')
parser.add_argument('-q', '--queue', choices=['borg1', 'borg2', 'borg3', 'borg-test'], required=True, help='Queue to submit to.')
parser.add_argument('-n', '--nproc', type=int, required=True, help='Number of processors.')
parser.add_argument('-v', '--version', choices=['16'], help='Version of the software you want to use.')
parser.add_argument('-s', '--noscr', action='store_true', help="Scratch won't be erased after 24 hours without writing.")
parser.add_argument('-w', '--walltime', help='Custom walltime in seconds. Borg1-max: 10800000, Borg-2 max: 21600000, Borg-3 max: -, Borg-test max: 129600')
parser.add_argument('-N', '--nosub', action='store_true', help='Do not submit. Only crate script.pbs file.')
parser.add_argument('input', help='Input file name.')
parser.add_argument('output', help='Output file name.')
args = parser.parse_args()

def configureGeneral():
    if args.nproc == 1:
        pbsnodes = '\n#PBS -l nodes='+str(args.nproc)+':'+args.queue

    elif args.nproc > 1:
        pbsnodes = '\n#PBS -l nodes=1:'+args.queue+':ppn='+str(args.nproc)

    return pbsnodes


def configureScratch():
    if args.noscr:
        doNotDeleteScratch = 'touch NO_ESBORRAR_SCRATCH'

    else:
        doNotDeleteScratch = ''

    return doNotDeleteScratch


def configureQueue():
    if args.queue == 'borg1':
        walltime = '\n#PBS -l walltime='+str(int(secondsBorg1/args.nproc))

    elif args.queue == 'borg2':
        walltime = '\n#PBS -l walltime='+str(int(secondsBorg2/args.nproc))

        if args.nproc < 12:
            print('ERROR: No less than 12 cores in borg2')
            sys.exit(1)

    elif args.queue == 'borg3':
        walltime = ''

    elif args.queue == 'borg-test':
        walltime = '\n#PBS -l walltime='+str(int(secondsBorgTest/args.nproc))

        if args.nproc > 8:
            print('ERROR: Maximum of 8 cores in borg-test')
            sys.exit(1)

    if args.walltime:
        walltime = '\n#PBS -l walltime='+str(int(args.walltime/args.nproc))

    return walltime


def configureVersion():
    if args.version:
        version = args.version

    else:
        version = '16-C.01'

    if args.nproc == 1:
        executable = program

    else:
        if args.queue == 'borg2':    
            executable = 'mpirun -np '+str(args.nproc)+' '+program

        elif args.queue == 'borg3' or args.queue == 'borg-test':
            executable = 'mpirun -np '+str(args.nproc)+' '+program

    return version, executable


def configureFiles():
    if not os.path.isfile('./'+args.input):
        print('ERROR: '+args.input+" doesn't exists or isn't a file\n")
        sys.exit(1)
    else:
        with open(args.input, 'r') as inputFile:
            for line in inputFile:
                if '%nproc='+str(args.nproc) in line:
                    break

                elif '%nproc=' in line:
                    print('ERROR: You are asking for '+str(args.nproc)+' cores and you are setting '+line.rstrip()+' in your input file. This two values have to match.\nPlease correct it.\n')
                    inputFile.close()
                    sys.exit()

            else:
                print('ERROR: No %nproc=n line in your input file. Add a line with this parameter where n is the number of cores.\nPlease correct it.\n')
                inputFile.close()
                sys.exit(1)
                
    if args.output == './':
        output = Path(args.input).with_suffix('.qfi')
    
    else:
        output = args.output

    return output

def configureModule(version):
    # gaussian/16-C.01
    return program+'/'+version


def makeFile(pbsnodes, walltime, module, doNotDeleteScratch, version, executable, output):

    template = """#PBS -q {queue}
#PBS -N {input}
#PBS -M {user}@klingon.uab.cat{pbsnodes}{walltime}
#PBS -k oe
#PBS -r n

### ENVIRONMENT ### 
. /QFcomm/environment.bash
module load {module}
{doNotDeleteScratch}

### EXECUTION ###
date > $PBS_O_WORKDIR/{output}
cat $PBS_NODEFILE >> $PBS_O_WORKDIR/{output}
{executable} < $SWAP_DIR/{input} >> $PBS_O_WORKDIR/{output}
date >> $PBS_O_WORKDIR/{output}
"""

    context = {
        "queue": args.queue,
        "user": user,
        "nproc": args.nproc,
        "input": args.input,
        "output": output,
        "pbsnodes": pbsnodes,
        "walltime": walltime,
        "module": module,
        "doNotDeleteScratch": doNotDeleteScratch,
        "version": version,
        "executable": executable,
    }

    with open(filename, 'w') as file:
        file.write(template.format(**context))


def jobInformation(user, module):
    print('')
    print('--- Script information ---')
    print('Hostname:'+hostname)
    print('Username: '+user)
    print('Modules: '+module)


def submitJob():
    if args.nosub == False:
        os.system("/usr/local/torque/bin/qsub script.pbs")
        print('Job sent to '+args.queue+'\n')
    else:
        print(filename+' created.\n')


def main():
    pbsnodes = configureGeneral()
    doNotDeleteScratch = configureScratch()
    walltime = configureQueue()
    version, executable = configureVersion()
    output = configureFiles()
    module = configureModule(version)
    makeFile(pbsnodes, walltime, module, doNotDeleteScratch, version, executable, output)
    jobInformation(user, module)
    submitJob()
main()
