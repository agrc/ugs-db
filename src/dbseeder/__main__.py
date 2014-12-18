# -*- coding: utf-8 -*-

import argparse
import sys
from dbseeder import Seeder


def main(argv=()):
    """
    Args:
        argv (list): List of arguments

    Returns:
        int: A return code

    Does stuff.
    """
    print(argv)

    argv = sys.argv

    print(argv)

    seeder = Seeder()

    parser = argparse.ArgumentParser(description='seed a geodatabse.')

    parser.add_argument(
        '--update', action='store_true', help='update the gdb')
    parser.add_argument(
        '--seed', nargs='*', help='seed the gdb from a datasource on disk')
    parser.add_argument(
        '--length', nargs='*', help='get the max field sizes from files on disk. --length program featureclass')
    parser.add_argument(
        '--relate', action='store_true',
        help='creates the releationship class between stations and results')

    args = parser.parse_args()

    location = 'c:\\temp\\'
    gdb = 'master.gdb'
    seed_data = '.\\data'

    from nose.tools import set_trace
    set_trace()

    try:
        if args.update:
            pass
        elif args.length:
            seeder = Seeder(location, gdb)
            maps = seeder.field_lengths(args.length)

            for key in maps.keys():
                print '{}'.format(maps[key])
        elif args.relate:
            seeder = Seeder(location, gdb)
            seeder.create_relationship()
        else:
            if args.seed is None:
                args.seed = ['Stations', 'Results']

            print 'seeding {} with {}'.format(gdb, args.seed)
            seeder = Seeder(location, gdb)
            seeder.seed(seed_data, args.seed)

        print 'finished'
    except:
        raise

    return 0

if __name__ == "__main__":
    sys.exit(main())
