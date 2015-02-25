#!/usr/bin/env python
"""
.. module:: esg_mapfiles.py
   :platform: Unix
   :synopsis: Build ESG-F mapfiles without esgscan_directory upon local ESG-F datanode.

.. moduleauthor:: Levavasseur, G. <glipsl@ipsl.jussieu.fr>


"""

# Module imports
import os, re, sys, argparse, ConfigParser, logging
from multiprocessing.dummy import Pool as ThreadPool
from argparse import RawTextHelpFormatter
from lockfile import LockFile, LockTimeout
from datetime import datetime
from itertools import izip, repeat
from tempfile import mkdtemp
from shutil import copy2, rmtree


# Program version
__version__ = '{0} {1}-{2}-{3}'.format('v0.2', '2015', '02', '12')


class _ProcessingContext(object):
    """Encapsulate processing context information for main process."""
    def __init__(self, args):
        self.outdir = args.outdir
        self.logdir = args.logdir
        # If None -> No log
        # If Empty -> Log as constant value
        # Else -> Log as supplied
        if self.logdir:
            _init_logger(self.logdir)
        self.verbose = args.verbose
        self.directory = os.path.normpath(args.directory)
        self.project = args.project
        self.dataset = args.per_dataset
        self.outmap = args.mapfile
        self.dtemp = mkdtemp()
        # Check if configuration file exists
        if not os.path.isfile(args.config):
            if self.logdir:
                logging.error('Configuration file not found') 
            raise Exception('Configuration file not found')
        else:
            self.cfg = ConfigParser.ConfigParser()
            self.cfg.read(args.config)
            self.pattern = re.compile(self.cfg.get(self.project, 'directory_format'))


def _get_args():
    """Returns parsed command line arguments."""
    parser = argparse.ArgumentParser(
                        description="""Build ESG-F mapfiles upon local ESG-F datanode bypassing esgscan_directory\ncommand-line.""",
                        formatter_class = RawTextHelpFormatter,
                        add_help=False,
                        epilog = "Developped by Levavasseur, G. (CNRS/IPSL)")
    parser.add_argument('directory',
                        type=str,
                        nargs = '?',
                        help = 'Directory to recursively scan')
    parser.add_argument('-h', '--help',
                        action = "help",
                        help = """Show this help message and exit.\n\n""")
    parser.add_argument('-p', '--project',
                        type = str,
                        choices = ['cmip5', 'cordex'],
                        required = True,
                        help="""Required project to build mapfiles among:\n- cmip5\n- cordex\n\n""")
    parser.add_argument('-c','--config',
                        type = str,
                        default = '{0}/esg_mapfiles.ini'.format(os.getcwd()),
                        help = """Path of configuration INI file\n(default is '{workdir}/esg_mapfiles.ini').\n\n""") 
    parser.add_argument('-o','--outdir',
                        type = str,
                        default = os.getcwd(),
                        help = """Mapfile(s) output directory\n(default is working directory).\n\n""")
    parser.add_argument('-l', '--logdir',
                        type = str,
                        nargs = '?',
                        const = os.getcwd(),
                        help = """Logfile directory (default is working directory).\nIf not, standard output is used.\n\n""")
    parser.add_argument('-m','--mapfile',
                        type = str,
                        default = 'mapfile.txt',
                        help = """Output mapfile name. Only used without --per-dataset option\n(default is 'mapfile.txt').\n\n""")
    parser.add_argument('-d','--per-dataset',
                        action = 'store_true',
                        default = False,
                        help = """Produce ONE mapfile PER dataset.\n\n""")
    parser.add_argument('-v', '--verbose',
                        action = 'store_true',
                        default = False,
                        help = """Verbose mode.\n\n""")
    parser.add_argument('-V', '--Version',
                        action = 'version',
                        version = "%(prog)s ({0})".format(__version__),
                        help = """Program version.""")
    return parser.parse_args()


def _get_unique_logfile(logdir):
    """Get unique logfile name."""
    logfile = 'esg_mapfile-{0}-{1}.log'.format(datetime.now().strftime("%Y%m%d-%H%M%S"), os.getpid())
    return os.path.join(logdir, logfile)


def _init_logger(logdir):
    """Creates logfile"""
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    logfile = _get_unique_logfile(logdir)
    logging.basicConfig(filename = logfile, level = logging.DEBUG, format = '%(asctime)s %(levelname)s %(message)s', datefmt = '%Y/%m/%d %I:%M:%S %p')


def _get_files(directory):
    """Yields file walinking DRS tree ignoring 'files' or 'latest' directories."""
    for root, dirs, files in os.walk(directory):
        if 'files' in dirs:
            dirs.remove('files')
        if 'latest' in dirs:
            dirs.remove('latest')
        for file in files:
            yield os.path.join(root, file)


def _get_master_ID(attributes, config):
    """Returns master ID and version from dataset path."""
    facets = config.get(attributes['project'].lower(), 'dataset_ID').replace(" ", "").split(',')
    master_ID = [attributes['project'].lower()]
    for facet in facets :
        master_ID.append(attributes[facet])
    return '.'.join(master_ID)


def _get_options(section, facet, config):
    """Get facet options of a section as list."""
    return config.get(section, '{0}_options'.format(facet)).replace(" ", "").split(',')


def _check_facets(attributes, logdir, config):
    """Check all attribute regarding controlled vocabulary."""
    for facet in attributes.keys() :
        if not facet in ['project','filename','variable','root','version','ensemble'] :
            options = _get_options(attributes['project'].lower(), facet, config)
            if not attributes[facet] in options :
                if logdir:
                    logging.error('"{0}" not in "{1}_options". Skip file {2}'.format(attributes[facet], facet, file)) 
                raise Exception('"{0}" not in "{1}_options". Skip file {2}'.format(attributes[facet], facet, file))


def _checksum(file, logdir):
    """Do MD5 checksum by Shell"""
    # Here 'md5sum' from Unix filesystem is used avoiding python memory limits
    try:
        shell = os.popen("md5sum {0} | awk -F ' ' '{{ print $1 }}'".format(file), 'r')
        return shell.readline()[:-1]
    except:
        if logdir:
            logging.error('Checksum failed for {0}'.format(file))
        raise Exception('Checksum failed for {0}'.format(file))


def _wrapper(args):
    """Pool map pultiple arguments wrapper."""
    return _process(*args)


def _process(file, ctx): 
    """File processing."""
    # Matching file full path with corresponding project pattern
    try: 
        attributes = re.match(ctx.pattern, file).groupdict()
    except:
        # Fails can be due to:
        # -> Wrong project argument
        # -> Mistake in directory_format pattern in configuration file
        # -> Wrong version format (defined as one or more digit after 'v')
        # -> Wrong ensemble format (defined as r[0-9]i[0-9]p[0-9])
        # -> Mistake in the DRS tree of your filesystem.
        if ctx.logdir:
            logging.error('Matching failed for {0}'.format(file)) 
        raise Exception('Matching failed for {0}'.format(file))
    # Control vocabulary of each facet
    _check_facets(attributes, ctx.logdir, ctx.cfg)
    # Deduce master ID from fulle path
    master_ID = _get_master_ID(attributes, ctx.cfg)
    (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(file)
    # Make the file checksum (MD5)
    checksum = _checksum(file, ctx.logdir)
    # Build mapfile name if one per dataset, or not
    if ctx.dataset:
        outmap = '{0}.{1}'.format(master_ID, attributes['version'])
    else:
        outmap = ctx.outmap
    outfile = os.path.join(ctx.dtemp, outmap)
    # Generate a lock file to avoid that several threads write on the same file at the same time
    # LockFile is acquire and release after writing.
    # Acquiring LockFile is timeouted if it's locked by other thread.
    # Each process add one line to the appropriate mapfile 
    lock = LockFile(outfile)
    try:
        lock.acquire(timeout = int(ctx.cfg.defaults()['lockfile_timeout']))
        f = open(outfile,'a+')
        f.write('{0} | {1} | {2} | mod_time={3} | checksum={4} | checksum_type={5}\n'.format(master_ID, file, size, str(mtime)+'.000000', checksum, 'MD5'))
        f.close()
        lock.release()
    except LockTimeout:
        raise Exception('Timeout exceeded. Skip file {0}'.format(file))
    if ctx.logdir:
        logging.info('{0} <-- {1}'.format(outmap, file))
    else:
        sys.stdout.write('{0} <-- {1}\n'.format(outmap, file))
    return outmap


def main():
    """Main entry point."""
    # Processing context initialization
    ctx = _ProcessingContext(_get_args())
    # Create output directory if not exists
    if not os.path.isdir(ctx.outdir) :
        if ctx.verbose :
            if ctx.logdir:
                logging.info('Create output directory: {0}'.format(ctx.outdir))
            else:
                sys.stdout.write('Create output directory: {0}\n'.format(ctx.outdir))
        os.mkdir(ctx.outdir)
    if ctx.logdir:
        logging.info('Scan started for {0}'.format(ctx.directory))
    else:
        print 'Scan started for {0}'.format(ctx.directory)
    # Start threads pool over files list in supplied directory
    pool = ThreadPool(int(ctx.cfg.defaults()['threads_number']))
    # Return the list of generated mapfiles in temporary directory 
    outmaps = pool.map(_wrapper, izip(_get_files(ctx.directory), repeat(ctx)))
    pool.close()
    pool.join()
    if ctx.logdir:
        logging.info('Scan completed for {0}'.format(ctx.directory))
    else:
        sys.stdout.write('Scan completed for {0}\n'.format(ctx.directory))
    # Overwrite each existing mapfile in output directory
    for outmap in list(set(outmaps)):
        copy2(os.path.join(ctx.dtemp, outmap), ctx.outdir)
    # Remove temporary directory
    if ctx.verbose:
        if ctx.logdir:
            logging.info('Delete temporary directory {0}'.format(ctx.dtemp))
        else:
            sys.stdout.write('Delete temporary directory {0}\n'.format(ctx.dtemp))
    rmtree(ctx.dtemp)


# Main entry point
if __name__ == "__main__":
    main()
