#ifndef __TPOOL_H__
#define __TPOOL_H__

#include <stddef.h>
#include <stdlib.h>
#include <pthread.h>

typedef struct tpool_work tpool_work;
typedef struct tpool tpool;

typedef void (*thread_func_t)(void *arg);

/**
 * @brief Create a pool of threads
 *
 * @param size Size of the pool
 * @return tpool*
 */
tpool *tpool_create(size_t size);

/**
 * @brief Stop threads. Each thread will finish its work and then terminate
 *
 * @param t
 * @param size
 */
void tpool_stop(tpool *t);

/**
 * @brief Wait for all threads to terminate and destroy the pool of threads
 *
 * @param t
 * @param size
 */
void tpool_destroy(tpool *t);

/**
 * @brief Work of a thread
 *
 * @param args
 */
void *tpool_worker(void *args);

/**
 * @brief Assign work to a worker
 *
 * @param t Pool of threads
 * @param worker_id Worker id
 * @param func Work to add
 * @param args
 */
void tpool_add_work(tpool *t, int worker_id, thread_func_t func, void *args);

/**
 * @brief Wait for all works to be completed
 *
 * @param t Pool of threads
 * @param size Size of the pool
 */
void tpool_wait_all(tpool *t);

/**
 * @brief Wait for the worker to be completed
 *
 * @param t Pool of threads
 * @param size Size of the pool
 * @param worker_id The worker to wait for
 */
void tpool_wait_one(tpool *t, size_t worker_id);

#endif /* __TPOOL_H__ */
