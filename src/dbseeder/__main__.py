#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''UGS Chemistry database seeder
Usage:
  dbseeder create-tables <configuration>
  dbseeder seed <source> <file_location> <configuration>
  dbseeder update <source> <configuration>
  dbseeder postprocess <configuration>
  dbseeder (-h | --help | --version)
Options:
  -h --help     Show this screen.
  <configuration> dev, stage, prod
  <source> WQP, SDWIS, DOGM, DWR, UGS
  <file_location> the parent location of the programs data
'''

import sys
from dbseeder import Seeder
from docopt import docopt


def main():
    arguments = docopt(__doc__, version='1.0.0')
    seeder = Seeder()

    if arguments['seed']:
        return seeder.seed(source=arguments['<source>'], file_location=arguments['<file_location>'], who=arguments['<configuration>'])
    elif arguments['update']:
        return seeder.update(source=arguments['<source>'], who=arguments['<configuration>'])
    elif arguments['create-tables']:
        return seeder.create_tables(who=arguments['<configuration>'])
    elif arguments['postprocess']:
        return seeder.post_process(who=arguments['<configuration>'])

if __name__ == '__main__':
    sys.exit(main())
