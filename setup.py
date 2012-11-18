from setuptools import setup

setup(
    name="padlock",
    version="0.0.1",
    url='http://github.com/samuraisam/padlock',
    author='Samuel Sutch',
    author_email='samuel.sutch@gmail.com',
    description='A lock library with multiple backends',
    packages=['padlock'],
    install_requires=[
        'zope.interface==4.0.1',
        'zope.component==4.0.0',
        'zope.configuration==4.0.0',
        'time_uuid==0.1.0'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ]
)
