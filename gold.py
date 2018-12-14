#! /usr/bin/env python3.5
# -*- coding: utf-8 -*-

# This file allows to submit gold jobs to the kirk cluster
# File: gold.py
# Author: Sergi Pérez Labernia, 2018.

# Imports
import os
import sys
from argparse import ArgumentParser

# Global definitions
filename = 'script.pbs'
program = 'gold'
secondsBorg1 = 10800000
secondsBorg2 = 21600000
secondsBorg3 = None
secondsBorgTest = 129600
user = os.getlogin()
hostname = ''

# Arguments
parser = ArgumentParser(description='gold.py allows to summit GOLD jobs to the kirk cluster.')
parser.add_argument('-q', '--queue', choices=['borg1', 'borg2', 'borg3', 'borg-test'], required=True, help='Queue to submit to.')
parser.add_argument('-n', '--nproc', type=int, required=True, help='Number of processors.')
parser.add_argument('-v', '--version', choices=['2018'], help='Version of the software you want to use.')
parser.add_argument('-s', '--noscr', action='store_true', help="Scratch won't be erased after 24 hours without writing.")
parser.add_argument('-w', '--walltime', help='Custom walltime in seconds. Borg1-max: 10800000, Borg-2 max: 21600000, Borg-3 max: -, Borg-test max: 129600')
parser.add_argument('-N', '--nosub', action='store_true', help='Do not submit. Only crate script.pbs file.')
parser.add_argument('-m', '--multinode', action='store_true', help='Allow multinode execution,')
parser.add_argument('input', help='Input file name.')
args = parser.parse_args()

# Configurations
def configureGeneral():
    if args.nproc == 1:
        pbsnodes = '\n#PBS -l nodes='+str(args.nproc)+':'+args.queue

    elif args.nproc > 1:
        if args.multinode == True:
            pbsnodes = '\n#PBS -l nodes= 1:'+args.queue+':ppn='+str(args.nproc)
        else:
            pbsnodes = '\n#PBS -l nodes='+str(args.nproc)

    return pbsnodes

def configureScratch():
    if args.noscr:
        doNotDeleteScratch = 'touch NO_ESBORRAR_SCRATCH'

    else:
        doNotDeleteScratch = ''
    
    if args.multinode == False:
        filesToTemp = '''
if [ ! -d "$SWAP_DIR" ]; then
    mkdir -p $SWAP_DIR || exit $?
    cp -r $PBS_O_WORKDIR/* $SWAP_DIR || exit $?
    cd $SWAP_DIR
fi'''
    else:
        filesToTemp = '''
machines=$PBS_O_WORKDIR/machinefile
rm $machines hosts
cat $PBS_NODEFILE | perl -pe 's/.uab.es//g'>$machines
sort $machines | uniq > hosts

for node in `cat hosts`
do
    ssh $node "mkdir $SWAP_DIR"
    ssh $node "cp $PBS_O_WORKDIR/* $SWAP_DIR/"
done
cd $SWAP_DIR
'''

    return doNotDeleteScratch, filesToTemp

def configureQueue():
    if args.queue == 'borg1':
        walltime = int(secondsBorg1/args.nproc)

    elif args.queue == 'borg2':
        if args.nproc < 12:
            print('ERROR: No less than 12 cores in borg2')
            sys.exit(1)

        walltime = int(secondsBorg2/args.nproc)

    elif args.queue == 'borg-test':
        if args.nproc > 8:
            print('ERROR: Maximum of 8 cores in borg-test')
            sys.exit(1)

        walltime = int(secondsBorgTest/args.nproc)

    if args.walltime:
        walltime = args.walltime/args.nproc

    return walltime

def configureVersion():
    if args.version:
        version = 'Enterprise'+args.version
    else:
        version = 'Enterprise2018'

    exec = program+'_auto'
    return version, exec

def configureFiles():  
    if not os.path.isfile('./'+args.input):
        print('ERROR: '+args.input+" doesn't exists or isn't a file")
        sys.exit(1)

def configureModule(version):
    return 'CSD/'+version
    
def makeFile(pbsnodes, walltime, module, filesToTemp, doNotDeleteScratch, version, exec):

    template = """#PBS -q {queue}
#PBS -N {input}
#PBS -M {user}@klingon.uab.cat{pbsnodes}
#PBS -l walltime={walltime}
#PBS -k oe
#PBS -r n

### ENVIRONMENT ###	
. /QFcomm/modules.profile 
module load {module}

JOB_ID=${{PBS_JOBID%'.kirk.uab.es'}}
SWAP_DIR=/scratch/{user}/$JOB_ID
{filesToTemp}
{doNotDeleteScratch}
### EXECUTION ###
{exec} {input}

### RESULTS ###
cp -f $SWAP_DIR/* $PBS_O_WORKDIR/$JOB_ID"""

    context = {
        "queue": args.queue,
        "user": user,
        "nproc": args.nproc,
        "input": args.input,
        "pbsnodes": pbsnodes,
        "walltime": walltime,
        "module": module,
        "filesToTemp": filesToTemp,
        "doNotDeleteScratch": doNotDeleteScratch,
        "version": version,
        "exec": exec,
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
    doNotDeleteScratch, filesToTemp = configureScratch()
    walltime = configureQueue()
    version, exec = configureVersion()
    configureFiles()
    module = configureModule(version)
    makeFile(pbsnodes, walltime, module, filesToTemp, doNotDeleteScratch, version, exec)
    jobInformation(user, module)
    submitJob()
main()
