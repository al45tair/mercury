from distutils.core import setup, Extension

__version__ = '0.1'

setup(name='mercury',
      version=__version__,
      description='Pythonic API for Mercurial',
      license='MIT',
      long_description="""
mercury is similar in purpose to python-hglib, but tries to provide a more
powerful and more Pythonic interface to the SCM system.
""",
      author='Alastair Houghton',
      author_email='alastair@alastairs-place.net',
      packages=['mercury'],
      package_dir = { '': 'src' },
      ext_package='mercury',
      ext_modules=[Extension('base85', sources=['src/mercury/base85.c'])],
      requires=['mercurial',
                'hglist'])
