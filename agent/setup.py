from setuptools import setup

setup(
    name='temboard-agent-backup',
    version='0.1',
    author='Nicolas Thauvin',
    author_email='nicolas.thauvin@dalibo.com',
    license='PostgreSQL',
    install_requires=['temboard-agent'],
    py_modules=['temboard_agent_backup'],
    entry_points={
        'temboardagent.plugins': [
            'backup = temboard_agent_backup:BackupPlugin',
        ],
    },
)
