#include "tpool.h"

typedef struct tpool_work
{
	thread_func_t function;
	void *args;
	int running;
} tpool_work;

typedef struct tpool
{
	pthread_t *tids;
	size_t size;
	tpool_work *workers;
} tpool;

tpool *tpool_create(size_t size)
{
	int i;

	tpool *t = malloc(sizeof(tpool));
	t->size = size;
	t->tids = malloc(size * sizeof(pthread_t));

	// Create workers
	t->workers = malloc(size * sizeof(tpool_work));

	for (i = 0; i < size; i++)
	{
		t->workers[i].function = NULL;
		t->workers[i].running = 1;

		pthread_create(&t->tids[i], NULL, tpool_worker, (void *)&t->workers[i]);
	}

	return t;
}

void tpool_stop(tpool *t)
{
	int i;
	for (i = 0; i < t->size; i++)
		t->workers[i].running = 0;
}

void tpool_destroy(tpool *t)
{
	int i;
	for (i = 0; i < t->size; i++)
	{
		t->workers[i].running = 0;
		pthread_join(t->tids[i], NULL);
	}

	free(t->workers);
	free(t->tids);
	free(t);
}

void *tpool_worker(void *args)
{
	tpool_work *work;
	while (1)
	{
		work = (tpool_work *)args;
		if (work->function != NULL)
		{
			// Do work
			work->function(work->args);

			// Wait
			work->function = NULL;
		}

		else if (!work->running)
			break;
	}
	return NULL;
}

void tpool_add_work(tpool *t, int worker_id, thread_func_t func, void *args)
{
	if (worker_id >= t->size)
		return;

	t->workers[worker_id].function = func;
	t->workers[worker_id].args = args;
}

void tpool_wait_all(tpool *t)
{
	int i, res;

	// While workers have work (function != NULL), wait
	while (1)
	{
		res = 0;
		for (i = 0; i < t->size; i++)
			if (t->workers[i].function != NULL)
				res++;

		if (!res)
			break;
	}
}

void tpool_wait_one(tpool *t, size_t worker_id)
{
	if (worker_id >= t->size)
		return;

	while (1)
	{
		if (t->workers[worker_id].function == NULL)
			break;
	}
}
