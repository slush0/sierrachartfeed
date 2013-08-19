from distutils.core import setup
import sys, os

generic_args = {
      "name" : 'sierrachartfeed',
      "version" : '1.0',
      "description" : 'Sierrachart feed bridge for bitcoincharts.com',
      "author" : 'slush',
      "url" : 'https://github.com/slush0/sierrachartfeed'
}

if sys.platform.startswith('linux'):
    setup(
        py_modules=['sierrachartfeed'],
        packages=['scid'],
        **generic_args
    )
else:
    sys.argv.append('py2exe')

    setup(
        options = {'py2exe':
            {'optimize': 2,
            'bundle_files': 1,
            'compressed': True,
            },
        },
        console = ['sierrachartfeed.py'],
        zipfile = None,
        **generic_args
    )
