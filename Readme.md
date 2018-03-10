# Proto Memcash Loader

Otus homework project

### install:

clone repository:

    $ git clone https://github.com/assigdev/proto_memcash_loader
    
    pipenv install
    
if you don't have pipenv:
    
    pip install pipenv

### Run

up 4 memcashed in docker
    
    docker-compose up

program start:
    
    pipenv run python memc_load.py

in env:
    
    python memc_load.py

### Configs
    
    Options:
      -h, --help            show this help message and exit
      -t, --test            run test fixtures
      -l LOG, --log=LOG     Log file path
      --dry                 Debug mode
      --pattern=PATTERN     Pattern for files path
      --idfa=IDFA           memcash  ip:port for iphone ids
      --gaid=GAID           memcash  ip:port for android gaid
      --adid=ADID           memcash  ip:port for android adid
      --dvid=DVID           memcash  ip:port for android dvid
      -w WORKERS_COUNT, --workers_count=WORKERS_COUNT
                            count of workers
      --threads_count=THREADS_COUNT
                            count of threads
