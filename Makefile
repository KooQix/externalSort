CC=gcc
CFLAGS=-Wall -Wno-comment
src=src
files=$(src)/mysort.c $(src)/tpool.c
dest=bin

all: mysort

mysort: 
	$(CC) $(files) $(CFLAGS) -o $(dest)/$@ $<
	
clean:
	rm -rf $(dest)/mysort $(dest)/tpool
