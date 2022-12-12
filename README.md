# Multi-threading external sort (internal if data fits in memory)

## Gensort and valsort needs to be installed (generate and validate sorted data)

http://www.ordinal.com/gensort.html

Create a data file using

    $ ./gensort -a numLines outputFile

## Compile

Inside the directory where the Makefile is

    $ mkdir -p bin
    $ make clean; make

## Run

    $ bin/./mysort inputFile outputFile numThreads maxMemory debugMode

-   maxMemory: int, GB, min 1GB
-   debugMode: 0 | 1

## Run multiple mysort, validation and Linux sort

    $ sudo chmod +x run.sh
    $ mkdir -p ../data/data ../data/sorted

Create the data files inside data/data named "data_sizeGB.txt" where size GB is the size of the file \
Update last lines of run.sh as needed to set parameters and run:

    $ ./run.sh

