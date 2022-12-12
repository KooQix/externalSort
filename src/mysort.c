#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <string.h>
#include "tpool.h"

#define USE "./mySort <input file> <output file> <number of threads> <max memory (GB)> <debugMode 0|1>\nMin memory 1GB"
#define BUFFER_SIZE 100
#define MAX_MEMORY 200
#define TMP_FILE "tmp_sorted_chunks.txt"
#define MAX_LINES_FILE 2 * 10000000000 // <=> 2 TB (number of lines, 1 line = 100B)

// Args to give the mergeSort_multi function when multithreading
typedef struct args
{
	int tid;
	char **lines;
	int offset;
	unsigned long int chunkSize;
	int numThreads;
} mergeSort_args;

// Args to give the mergeChunks function when multithreading
typedef struct mergeChunks_args
{
	char **lines;
	int number;
	int aggregation;
	unsigned long int chunkSize;
	unsigned long int maxLength;
} mergeChunks_args;

//////////////////// Merge sort \\\\\\\\\\\\\\\\\\\\

void merge(char **lines, unsigned long int start, unsigned long int mean, unsigned long int end);
char **subset(char **lines, unsigned long int start, unsigned long int end);

/**
 * @brief Get 2 subsets from the input lines
 *
 * @param lines
 * @param start
 * @param end
 * @return char**
 */
char **subset(char **lines, unsigned long int start, unsigned long int end)
{
	unsigned long int i = start;

	char **res = malloc((end - start) * sizeof(char *));

	while (i < end)
	{
		res[i - start] = malloc(sizeof(char *) * BUFFER_SIZE);
		strcpy(res[i - start], lines[i]);
		i++;
	}
	return res;
}

/**
 * @brief Classic merge sort
 *
 * @param lines
 * @param start
 * @param end
 */
void mergeSort(char **lines, unsigned long int start, unsigned long int end)
{
	if (end - start == 1)
		return;

	unsigned long int mean = start + (end - start) / 2;
	mergeSort(lines, start, mean);
	mergeSort(lines, mean, end);
	merge(lines, start, mean, end);
}

void merge(char **lines, unsigned long int start, unsigned long int mean, unsigned long int end)
{
	unsigned long int i = 0;
	unsigned long int j = 0;

	unsigned long int length1 = mean - start;
	unsigned long int length2 = end - mean;

	char **left = subset(lines, start, mean);
	char **right = subset(lines, mean, end);

	for (unsigned long int k = 0; k < length1 + length2; k++)
	{
		if (i < length1 && j < length2)
		{
			if (strcmp(left[i], right[j]) < 0)
			{
				strcpy(lines[start + k], left[i]);
				free(left[i]);
				i++;
			}
			else
			{
				strcpy(lines[start + k], right[j]);
				free(right[j]);
				j++;
			}
		}
		else if (i < length1)
		{
			strcpy(lines[start + k], left[i]);
			free(left[i]);
			i++;
		}
		else
		{
			strcpy(lines[start + k], right[j]);
			free(right[j]);
			j++;
		}
	}

	free(left);
	free(right);
}

//////////////////// Multithreading \\\\\\\\\\\\\\\\\\\\

/**
 * @brief Merge sort //
 *
 * @param args
 */
void mergeSort_multi(void *args)
{
	mergeSort_args *m_args = (mergeSort_args *)args;

	unsigned long int start = m_args->tid * m_args->chunkSize;
	unsigned long int end = (m_args->tid + 1) * m_args->chunkSize;

	if (m_args->tid == m_args->numThreads - 1)
		end += m_args->offset;

	unsigned long int mean = (unsigned long int)start + (end - start) / 2;

	if (end - start == 1)
		return;

	mergeSort(m_args->lines, start, end);
	mergeSort(m_args->lines, start, end);
	merge(m_args->lines, start, mean, end);
}

/**
 * @brief Merge final chunks, which have been sorted by different threads
 *
 * @param lines
 * @param number
 * @param aggregation
 * @param chunkSize
 * @param maxLength
 */
void mergeChunks(void *_args)
{
	mergeChunks_args *args = (mergeChunks_args *)_args;

	for (int i = 0; i < args->number; i = i + 2)
	{
		int start = i * (args->chunkSize * args->aggregation);
		int end = ((i + 2) * args->chunkSize * args->aggregation);
		int mean = start + (args->chunkSize * args->aggregation);

		if (end > args->maxLength)
			end = args->maxLength;

		merge(args->lines, start, mean, end);
	}
	if (args->number / 2 >= 1)
	{
		args->number /= 2;
		args->aggregation *= 2;
		mergeChunks(args);
	}
}

//////////////////// External merge \\\\\\\\\\\\\\\\\\\\


/**
 * @brief K-merge sort (when external sort)
 *
 * @param lines
 * @param numChunks
 * @param numLinesFile
 * @param ftmp
 * @param fout
 * @param MAX_LINES_MEMORY
 */
void kMergeSort(char **lines, unsigned long int numChunks, unsigned long int numLinesFile, FILE *ftmp, FILE *fout, unsigned long int chunkSize, int debugMode)
{
	unsigned long int k, k_min;

	if (debugMode)
		printf("\nProcessing K-merge\n\n");

	// Open tmp file reading mode
	fclose(ftmp);
	ftmp = fopen(TMP_FILE, "r");

	unsigned long int indexes[numChunks];

	//////////////////// Init indexes & Read and load each line \\\\\\\\\\\\\\\\\\\\

	for (k = 0; k < numChunks; k++)
	{
		// Start line of each chunk
		indexes[k] = k * chunkSize;

		fseek(ftmp, indexes[k] * BUFFER_SIZE, SEEK_SET);
		fread(lines[k], sizeof(char), BUFFER_SIZE, ftmp);
	}

	//////////////////// K-merge sort \\\\\\\\\\\\\\\\\\\\

	while (1)
	{
		k_min = 0;
		while (k_min < numChunks && indexes[k_min] == NULL)
			k_min++;

		// Every sorted chunk has been handled => stop
		if (k_min == numChunks)
			break;

		// Compare and write min
		for (k = k_min + 1; k < numChunks; k++)
		{
			// Chunk already handled, continue to next
			if (indexes[k] == NULL)
				continue;

			if (strcmp(lines[k], lines[k_min]) < 0)
				k_min = k;
		}

		fwrite(lines[k_min], sizeof(char), BUFFER_SIZE, fout);
		indexes[k_min]++;

		// Chunk handled: set line value to -1
		if ((indexes[k_min] == (k_min + 1) * chunkSize) || (indexes[k_min] == numLinesFile))
			indexes[k_min] = NULL;
		else
		{
			// Update new line
			fseek(ftmp, indexes[k_min] * BUFFER_SIZE, SEEK_SET);
			fread(lines[k_min], sizeof(char), BUFFER_SIZE, ftmp);
		}
	}

	// Close ftmp and remove
	fclose(ftmp);
	remove(TMP_FILE);
}

//////////////////// Main \\\\\\\\\\\\\\\\\\\\

void mySort(char *inputFile, char *outputFile, int numThreads, unsigned long int MAX_LINES_MEMORY, int boolExternal, unsigned long int numLinesFile, tpool *tm, int debugMode)
{
	char *buffer;
	FILE *fin;
	FILE *fout;
	FILE *ftmp;

	mergeSort_args threads_args[numThreads];
	mergeChunks_args mergeChunks_args;

	char **lines;

	// Open input file
	fin = fopen(inputFile, "r");
	if (fin == NULL)
	{
		fprintf(stderr, "fopen(%s) failed", inputFile);
		return;
	}

	// Open output file
	fout = fopen(outputFile, "w");
	if (fout == NULL)
	{
		fprintf(stderr, "fopen(%s) failed", outputFile);
		return;
	}

	// If external sort, need a tmp file for sorted by chunks
	if (boolExternal)
	{
		ftmp = fopen(TMP_FILE, "w");
		if (ftmp == NULL)
		{
			fprintf(stderr, "fopen(tmp_file) failed");
			return;
		}
	}

	if (debugMode)
	{
		if (boolExternal)
			printf("--------------------- External Sort ---------------------\n\n");
		else
			printf("--------------------- Internal Sort ---------------------\n\n");
	}

	// Allocate memory for the buffer
	buffer = (char *)malloc(sizeof(char) * BUFFER_SIZE);
	int readSize;

	// Allocate memory for entries
	lines = malloc(sizeof(char *) * MAX_LINES_MEMORY);

	unsigned long int i, j;

	unsigned long int numChunks = 0;

	//////////////////// Read file \\\\\\\\\\\\\\\\\\\\

	while (1)
	{
		i = 0;

		if (debugMode)
		{
			if (boolExternal)
				printf("Processing chunk %ld\n", numChunks + 1);

			printf("Reading\n");
		}

		// If multiple chunks, next line has already been read => go back up 1 line
		if (numChunks > 0)
			fseek(fin, -1 * BUFFER_SIZE, SEEK_CUR);

		// Read and stack by chunks of size MAX_LINES_MEMORY
		while ((readSize = fread(&buffer[0], sizeof(char), BUFFER_SIZE, fin)) == BUFFER_SIZE)
		{
			if (i >= MAX_LINES_MEMORY)
				break;

			lines[i] = malloc(sizeof(char) * BUFFER_SIZE);
			strcpy(lines[i], buffer);
			i++;
		}

		numChunks++;

		//////////////////// Sort \\\\\\\\\\\\\\\\\\\\

		if (debugMode)
			printf("Sorting\n");

		// Multithreading
		if (numThreads > 1)
		{
			unsigned long int chunkSize = (unsigned long int)(i / numThreads);

			for (j = 0; j < numThreads; j++)
			{
				if (numChunks == 1)
				{
					threads_args[j].tid = j;
					threads_args[j].numThreads = numThreads;
				}
				threads_args[j].offset = (int)i % numThreads;
				threads_args[j].lines = lines;
				threads_args[j].chunkSize = chunkSize;

				// Give work to each thread
				tpool_add_work(tm, j, mergeSort_multi, (void *)&threads_args[j]);
			}

			// Wait for all threads to complete their work
			tpool_wait_all(tm);

			// Merge the sorted chunks
			mergeChunks_args.lines = lines;
			mergeChunks_args.number = numThreads;
			mergeChunks_args.aggregation = 1;
			mergeChunks_args.chunkSize = chunkSize;
			mergeChunks_args.maxLength = i;

			// Assign worker 0 the task to merge the sorted chunks
			tpool_add_work(tm, 0, mergeChunks, (void *)&mergeChunks_args);
			tpool_wait_one(tm, 0);
		}
		else
			mergeSort(lines, 0, i);

		//////////////////// Write \\\\\\\\\\\\\\\\\\\\

		if (debugMode)
			printf("Writing\n");

		j = 0;
		while (j < i)
		{
			// Write sorted chunk > tmp
			if (boolExternal)
				fwrite(lines[j], sizeof(char), BUFFER_SIZE, ftmp);
			else
				fwrite(lines[j], sizeof(char), BUFFER_SIZE, fout);

			j++;
		}

		// If EOF, break
		if (readSize != BUFFER_SIZE)
			break;
	}

	// Stop pool of threads before performing K-merge (if external)
	if (numThreads > 1)
		tpool_stop(tm);

	// K-merge sort (if external sort)
	if (boolExternal)
		kMergeSort(lines, numChunks, numLinesFile, ftmp, fout, MAX_LINES_MEMORY, debugMode);

	//////////////////// Free & close \\\\\\\\\\\\\\\\\\\\


	for (j = 0; j < MAX_LINES_MEMORY; j++)
		free(lines[j]);

	free(lines);

	// Clear buffer and close files
	free(buffer);
	fclose(fout);
	fclose(fin);
}

/**
 * @brief Get the size of the file
 *
 * @param fp
 * @return unsigned long int
 */
unsigned long int fsize(FILE *fp)
{
	unsigned long int prev = ftell(fp);
	fseek(fp, 0L, SEEK_END);
	unsigned long int sz = ftell(fp);
	fseek(fp, prev, SEEK_SET); // go back to where we were
	return sz;
}

int main(int argc, char **argv)
{
	char *inputFile;
	char *outputFile;
	int numThreads;
	struct timeval start, end;
	double executionTime;
	long double throughput;

	int memory, boolExternal, debugMode;
	tpool *tm;

	if (argc != 6)
	{
		fprintf(stderr, USE);
		return 1;
	}

	// Read arguments
	inputFile = argv[1];
	outputFile = argv[2];
	numThreads = atoi(argv[3]);
	memory = atoi(argv[4]);
	debugMode = atoi(argv[5]);

	if (debugMode != 0 && debugMode != 1)
	{
		fprintf(stderr, "Error: debugMode possible values are 0 and 1");
		return -1;
	}

	if (memory > MAX_MEMORY)
	{
		fprintf(stderr, "Error: Memory limit exceeded (%d) GB", MAX_MEMORY);
		return -1;
	}

	// 1 line = 100B, memory in GB = 1e9B
	unsigned long int MAX_LINES_MEMORY = memory * 10000000;

	// Check file size
	FILE *file = fopen(inputFile, "r");
	unsigned long int numLinesFile = fsize(file) / 100; // number of lines (1 line = 100B)
	fclose(file);

	// K-merge sort must be able to load 1 line of each sorted chunk in memory
	// max: number of chunks = max number of line given memory = memory * 1e7
	// <=> total lines file = memory * 1e14 (1GB = 1e7 lines)
	// for security, let's take memory * 1e13 (other variables will also need memory, in particular the array keeping record of the indexes during K-merge sort)
	unsigned long int maxLinesFile = memory * 10000000 * 1000000;

	if (numLinesFile >= maxLinesFile)
	{
		fprintf(stderr, "Error: File size limit exceeded (%lu) kB, for %d GB memory", (unsigned long int)(maxLinesFile / 10), memory);
		return -1;
	}

	// Avoid overflows
	if (numLinesFile > MAX_LINES_FILE)
	{
		fprintf(stderr, "Error: File size limit exceeded 2TB");
		return -1;
	}

	if (numLinesFile > MAX_LINES_MEMORY)
		boolExternal = 1;
	else
		boolExternal = 0;

	// Create pool of threads
	if (numThreads > 1)
		tm = tpool_create(numThreads);

	// Execute sort and measure execution time
	gettimeofday(&start, NULL);
	mySort(inputFile, outputFile, numThreads, MAX_LINES_MEMORY, boolExternal, numLinesFile, tm, debugMode);
	gettimeofday(&end, NULL);

	// Destroy pool of threads
	if (numThreads > 1)
		tpool_destroy(tm);

	executionTime = ((double)end.tv_sec - (double)start.tv_sec) + ((double)end.tv_usec - (double)start.tv_usec) / 1000000.0;

	throughput = (numLinesFile / 10000) / executionTime;

	printf("input file: %s\n", inputFile);
	printf("output file: %s\n", outputFile);
	printf("number of threads: %d\n", numThreads);
	printf("execution time: %lf\n", executionTime);
	printf("throughput: %Lf MB/s\n", throughput);

	return 0;
}