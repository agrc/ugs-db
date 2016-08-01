#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
UGS Chemistry database seeder
Usage:
  ugsdbseeder create-tables <configuration>
  ugsdbseeder seed <source> <file_location> <configuration>
  ugsdbseeder update <source> <configuration> [--file-location=<file_location>] [--post-process]
  ugsdbseeder postprocess <configuration>
  ugsdbseeder (-h | --help | --version)
Options:
  -h --help                         Show this screen.
  -v --version                      Show version.
  --file-location=<file_location>   The parent location of the programs data.
Argument values:
  <configuration>       dev, stage, prod
  <source>              WQP, SDWIS, DOGM, UDWR, UGS, or "" for all
  <file_location>       the parent location of the programs data "c:\data"
'''

import sys
from ugsdbseeder import Seeder
from docopt import docopt


def main():
    arguments = docopt(__doc__, version='1.1.0')
    seeder = Seeder()

    if arguments['seed']:
        return seeder.seed(source=arguments['<source>'],
                           file_location=arguments['<file_location>'],
                           who=arguments['<configuration>'])
    elif arguments['update']:
        return seeder.update(source=arguments['<source>'],
                             who=arguments['<configuration>'],
                             location=arguments['--file-location'],
                             postprocess=arguments['--post-process'])
    elif arguments['create-tables']:
        return seeder.create_tables(who=arguments['<configuration>'])
    elif arguments['postprocess']:
        return seeder.post_process(who=arguments['<configuration>'])

if __name__ == '__main__':
    sys.exit(main())
