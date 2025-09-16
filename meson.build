project('tll-odbc', 'cpp', version: '0.2.4', default_options: ['cpp_std=c++17', 'werror=true', 'optimization=2'])

include = [include_directories('src')]

tll = dependency('tll')
fmt = dependency('fmt')
odbc = dependency('odbc')

lib = shared_library('tll-odbc',
	['src/channel.cc'],
	include_directories : include,
	dependencies : [fmt, odbc, tll],
	install : true,
)

test('pytest', import('python').find_installation('python3')
	, args: ['-m', 'pytest', '-v', '--log-level=DEBUG', 'tests/']
	, env: 'BUILD_DIR=@0@'.format(meson.current_build_dir())
	, workdir: meson.current_source_dir()
)

install_data('src/odbc.yaml', install_dir : get_option('datadir') / 'tll/scheme/tll')
