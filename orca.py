#! /usr/bin/env python3.5
# -*- coding: utf-8 -*-

# This file allows to submit orca jobs to the kirk cluster
# File: orca.py
# Author: Sergi Pérez Labernia, 2018.

# Imports
from argparse import ArgumentParser
import os
import sys

# Global definitions
filename = 'script.pbs'
program = 'orca'
secondsBorg1 = 10800000
secondsBorg2 = 21600000
secondsBorg3 = None
secondsBorgTest = 129600
user = os.getlogin()
hostname = os.uname()[1]
program = 'orca'
compiler = ''
mpi = ''

# Arguments
parser = ArgumentParser( description = 'orca.py allows to summit ORCA jobs to kirk cluster. ')
parser.add_argument('-q', '--queue', choices = ['borg1','borg2','borg3','borg-test'], required = True, help = 'Queue to submit to.')
parser.add_argument('-n', '--nproc', type = int, required = True, help = 'Number of processors.')
parser.add_argument('-v', '--version', choices = ['2.7','2.8','3','4','4.1.2','4.2.1'], help = 'Version of the software you want to use.')
parser.add_argument('-s', '--noscr', action='store_true', help = "Scratch won't be erased after 24 hours without writing.")
parser.add_argument('-w', '--walltime', help = 'Custom walltime in seconds. Borg1-max: 10800000, Borg-2 max: 21600000, Borg-3 max: -, Borg-test max: 129600')
parser.add_argument('-N', '--nosub', action='store_true', help = 'Do not submit. Only crate script.pbs file.' )
parser.add_argument('input', help = 'Input file name.')
parser.add_argument('output', help = 'Output file name.')
args = parser.parse_args()


def configureGeneral():
    if args.nproc == 1:
        pbsnodes = '\n#PBS -l nodes='+str(args.nproc)+':'+args.queue

    elif args.nproc > 1:
        pbsnodes = '\n#PBS -l nodes=1:'+args.queue+':ppn='+str(args.nproc)

    memory = '\n#PBS -l mem='+str(args.nproc*4)+'GB'

    return pbsnodes, memory

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
        # by default, last one
        version = '4.2.1'

    if version == '2.7':
        print ('ORCA version '+args.version+' is not avaible in this system.')
        sys.exit()

    elif version == '2' or version == '2.8' or version == '2.8.0':
        compiler = 'intel2011'
        mpi = 'openmpi/1.8.1'
        version = '2.8.0'

    elif version == '3.0.2':
        compiler = 'intel2011'
        mpi = 'openmpi/1.8.1'
        version = '3.0.2'

    elif version == '3' or version == '3.0' or version == '3.0.3':
        compiler = 'intel2011'
        mpi = 'openmpi/1.8.1'
        version = '3.0.3'

    elif version == '3' or version == '3.0.2':
        compiler = 'intel2011'
        mpi = 'openmpi/1.8.1'
        version = '3.0.2'

    else:
        compiler = 'gcc'
        mpi = 'openmpi'

    return compiler, mpi, version


def configureFiles():
    if not os.path.isfile('./'+args.input):
        print ('ERROR: '+args.input+" doesn't exists or isn't a file" ) 
        sys.exit()

    elif args.nproc > 1:
        if not 'Opt PAL' or not 'nproc' in open(args.input).read():
            print('ERROR: Add Opt PAL {} or %pal nproc = {} in your input file. See Orca Job script page in wiki.qf.uab.cat '.format(args.nproc, args.nproc))
            sys.exit()
                
    if args.output == './':
        output = Path(args.input).with_suffix('.qfi')
    
    else:
        output = args.output

    return output


def configureModule (compiler, mpi, version):
    module = program+'/'+version    

    # cal afegir module ompi si no es la v 4
    if version in ['2.7', '2.8', '3']:
        if args.queue == 'borg-test': 
            queue = 'borg3'
        else: 
            queue = args.queue

        module2 = 'module load '+mpi+'_'+compiler+'-'+queue
    
    else:
        module2 = ''

    return program+'/'+version, module2


def makeFile(pbsnodes, walltime, memory, module, module2, doNotDeleteScratch, version, program, output):
        
    template= """#PBS -q {queue}
#PBS -N {input}
#PBS -M {user}@klingon.uab.cat{pbsnodes}{walltime}{memory}
#PBS -k oe
#PBS -r n

### ENVIRONMENT ### 
. /QFcomm/environment.bash
module load {module}
{module2}
{doNotDeleteScratch}
### EXECUTION ###
exec=`which {program}`

echo $SWAP_DIR > $PBS_O_WORKDIR/{output}
echo "********" >> $PBS_O_WORKDIR/{output}
cat $PBS_NODEFILE >> $PBS_O_WORKDIR/{output}
echo "********" >> $PBS_O_WORKDIR/{output}
$exec {input} >> $PBS_O_WORKDIR/{output}
cp $SWAP_DIR/*.gbw $PBS_O_WORKDIR/
cp $SWAP_DIR/*.txt $PBS_O_WORKDIR/
cp $SWAP_DIR/*.loc $PBS_O_WORKDIR/
cp $SWAP_DIR/*.qro $PBS_O_WORKDIR/
cp $SWAP_DIR/*.uno $PBS_O_WORKDIR/
cp $SWAP_DIR/*.unso $PBS_O_WORKDIR/
cp $SWAP_DIR/*.xyz $PBS_O_WORKDIR/
cp $SWAP_DIR/*.prop $PBS_O_WORKDIR/
""" 

    context = {
        'queue': args.queue,
        'user': user,
        'nproc': args.nproc,
        'input': args.input,
        'output': output,
        'pbsnodes': pbsnodes,
        'walltime': walltime,
        'memory': memory,
        'module': module,
        'module2': module2,
        'doNotDeleteScratch': doNotDeleteScratch,
        'version': version,
        'program': program,
    }

    with open(filename,'w') as file:
        file.write(template.format(**context))


def showInformation (hostname, user, module, module2):
    print ( '' )
    print ( '--- Script information ---' )
    print ( 'Hostname : '+hostname )
    print ( 'Username: '+user )
    print ( 'Modules: '+module+' '+module2 )


def submitJob ():
    if args.nosub == False:
        os.system("/usr/local/torque/bin/qsub script.pbs")
        print ('Job sent to '+args.queue+'\n')
    else: print (filename+' created.\n')


def main():
    pbsnodes, memory = configureGeneral()
    doNotDeleteScratch = configureScratch()
    walltime = configureQueue()
    compiler, mpi, version = configureVersion()
    output = configureFiles()
    module, module2 = configureModule(compiler, mpi, version)
    makeFile(pbsnodes, walltime, memory, module, module2, doNotDeleteScratch, version, program, output)
    showInformation(hostname, user, module, module2)
    submitJob()
main()
