extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.ifconfig',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode',
    'enum_tools.autoenum',
]
source_suffix = '.rst'
master_doc = 'index'
project = 'redis-throttled-queue'
year = '2022-2024'
author = 'Ionel Cristian Mărieș'
copyright = f'{year}, {author}'
version = release = '1.0.0'

pygments_style = 'trac'
templates_path = ['.']
extlinks = {
    'issue': ('https://github.com/ionelmc/python-redis-throttled-queue/issues/%s', '#%s'),
    'pr': ('https://github.com/ionelmc/python-redis-throttled-queue/pull/%s', 'PR #%s'),
}

html_theme = 'furo'
html_theme_options = {
    'githuburl': 'https://github.com/ionelmc/python-redis-throttled-queue/',
}

html_use_smartypants = True
html_last_updated_fmt = '%b %d, %Y'
html_split_index = False
html_short_title = f'{project}-{version}'

napoleon_use_ivar = True
napoleon_use_rtype = False
napoleon_use_param = False
