# coding: utf-8

import optparse
from Service import core
from Infra.CrossCutting import functions


def main():
    parser = optparse.OptionParser(add_help_option=False)

    parser.add_option("-t", "--time", dest="_time", type="int",
                      help="Synchronization Time(seconds)",)

    parser.add_option("-h", "--help",
                      action="store_true", dest="_help", help="-h")

    (options, args) = parser.parse_args()

    _time = options._time
    _help = options._help
    if not _help and not _time:
        print functions.printBasicInfo()

    if _help:
        print functions.printHelpMessage()
        return
    if _time:
        core.execute(_time)

if __name__ == "__main__":
    main()
