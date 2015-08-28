#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''UGS Chemistry database seeder
Usage:
  dbseeder createdb <configuration>
  dbseeder seed <source> <file_location> <configuration>
  dbseeder postprocess <configuration>
  dbseeder (-h | --help)
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
    arguments = docopt(__doc__)

    params = {
        'source': arguments['<source>'],
        'who': arguments['<configuration>']
    }

    seeder = Seeder()

    if arguments['seed']:
        params['file_location'] = arguments['<file_location>']

        return seeder.seed(**params)
    elif arguments['createdb']:
        return seeder.create_tables(who=params['who'])
    elif arguments['postprocess']:
        return seeder.post_process(who=params['who'])

if __name__ == '__main__':
    sys.exit(main())
