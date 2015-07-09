#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''UGS Chemistry database seeder
Usage:
  dbseeder createdb <configuration>
  dbseeder seed <source> <configuration>
  dbseeder length <source>
  dbseeder (-h | --help)
Options:
  -h --help     Show this screen.
  <configuration> dev, stage, prod
  <source> WQP, SDWIS, DOGM, DWR, UGS
'''

import sys
from dbseeder import Seeder
from docopt import docopt


def main():
    arguments = docopt(__doc__)

    args = {
        'source': arguments['<source>'],
        'who': arguments['<configuration>']
    }

    seeder = Seeder()

    if arguments['seed']:
        if not args['source']:
            args['source'] = ['WQP', 'SDWIS', 'DOGM', 'DWR', 'UGS']
        else:
            args['source'] = args['source'].split(',').map(strip)

        return seeder.seed(**args)
    elif arguments['createdb']:
        return seeder.create_tables(who=args['who'])


if __name__ == '__main__':
    sys.exit(main())
