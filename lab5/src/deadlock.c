#include <stdio.h>
#include <pthread.h>
#include <unistd.h> // For sleep()

pthread_mutex_t lock1;
pthread_mutex_t lock2;

// Thread 1 function
void* thread1_func(void* arg) {
    printf("Thread 1: Attempting to acquire lock1...\n");
    pthread_mutex_lock(&lock1);
    printf("Thread 1: Acquired lock1. Now attempting to acquire lock2...\n");
    sleep(1); // Simulate some work or delay
    pthread_mutex_lock(&lock2);
    printf("Thread 1: Acquired both locks!\n");

    // Release locks in reverse order of acquisition
    pthread_mutex_unlock(&lock2);
    printf("Thread 1: Released lock2.\n");
    pthread_mutex_unlock(&lock1);
    printf("Thread 1: Released lock1.\n");
    return NULL;
}

// Thread 2 function
void* thread2_func(void* arg) {
    printf("Thread 2: Attempting to acquire lock2...\n");
    pthread_mutex_lock(&lock2);
    printf("Thread 2: Acquired lock2. Now attempting to acquire lock1...\n");
    sleep(1); // Simulate some work or delay
    pthread_mutex_lock(&lock1);
    printf("Thread 2: Acquired both locks!\n");

    // Release locks in reverse order of acquisition
    pthread_mutex_unlock(&lock1);
    printf("Thread 2: Released lock1.\n");
    pthread_mutex_unlock(&lock2);
    printf("Thread 2: Released lock2.\n");
    return NULL;
}

int main() {
    pthread_t thread1, thread2;

    // Initialize mutexes
    pthread_mutex_init(&lock1, NULL);
    pthread_mutex_init(&lock2, NULL);

    // Create threads
    pthread_create(&thread1, NULL, thread1_func, NULL);
    pthread_create(&thread2, NULL, thread2_func, NULL);

    // Wait for threads to finish
    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);

    // Destroy mutexes
    pthread_mutex_destroy(&lock1);
    pthread_mutex_destroy(&lock2);

    printf("Main: Program finished.\n");
    return 0;
}