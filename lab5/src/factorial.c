#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <getopt.h>
#include <stdbool.h>

long long k = -1;
int pnum = -1;
long long mod = -1;
long long result = 1;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int thread_id;
    long long start;
    long long end;
} thread_data_t;

void* compute_range(void* arg) {
    thread_data_t* data = (thread_data_t*)arg;
    long long local_result = 1;
    
    for (long long i = data->start; i <= data->end; i++) {
        local_result = (local_result * i) % mod;
    }
    
    pthread_mutex_lock(&mutex);
    result = (result * local_result) % mod;
    pthread_mutex_unlock(&mutex);
    
    printf("Поток %d завершил вычисление диапазона [%lld, %lld]\n", 
           data->thread_id, data->start, data->end);
    
    return NULL;
}

int main(int argc, char **argv) {
    k = -1;
    pnum = -1;
    mod = -1;

    while (true) {
        int current_optind = optind ? optind : 1;

        static struct option options[] = {
            {"k", required_argument, 0, 0},
            {"pnum", required_argument, 0, 0},
            {"mod", required_argument, 0, 0},
            {0, 0, 0, 0}
        };

        int option_index = 0;
        int c = getopt_long(argc, argv, "", options, &option_index);

        if (c == -1) break;

        switch (c) {
            case 0:
                switch (option_index) {
                    case 0:
                        k = atoll(optarg);
                        if (k <= 0) {
                            printf("k must be a positive number\n");
                            return 1;
                        }
                        break;
                    case 1:
                        pnum = atoi(optarg);
                        if (pnum <= 0) {
                            printf("pnum must be a positive number\n");
                            return 1;
                        }
                        break;
                    case 2:
                        mod = atoll(optarg);
                        if (mod <= 0) {
                            printf("mod must be a positive number\n");
                            return 1;
                        }
                        break;
                    default:
                        printf("Index %d is out of options\n", option_index);
                }
                break;
            case '?':
                printf("Unknown option\n");
                break;
            default:
                printf("getopt returned character code 0%o?\n", c);
        }
    }

    if (optind < argc) {
        printf("Has at least one no option argument\n");
        return 1;
    }

    if (k == -1 || pnum == -1 || mod == -1) {
        printf("Usage: %s --k \"num\" --pnum \"num\" --mod \"num\"\n", argv[0]);
        return 1;
    }

    printf("Вычисление %lld! mod %lld\n", k, mod);
    printf("Количество потоков: %d\n", pnum);
    
    if (k == 0 || k == 1) {
        printf("Результат: 1\n");
        return 0;
    }
    
    if (pnum > k) {
        pnum = k;
        printf("Количество потоков уменьшено до %d (k = %lld)\n", pnum, k);
    }
    
    pthread_t threads[pnum];
    thread_data_t thread_data[pnum];
    
    long long range_size = k / pnum;
    long long remainder = k % pnum;
    
    long long current_start = 1;
    
    for (int i = 0; i < pnum; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].start = current_start;
        
        thread_data[i].end = current_start + range_size - 1;
        if (remainder > 0) {
            thread_data[i].end++;
            remainder--;
        }
        
        current_start = thread_data[i].end + 1;
        
        printf("Поток %d: диапазон [%lld, %lld]\n", 
               i, thread_data[i].start, thread_data[i].end);
        
        if (pthread_create(&threads[i], NULL, compute_range, &thread_data[i]) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
    
    for (int i = 0; i < pnum; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            perror("pthread_join");
            return 1;
        }
    }
    
    pthread_mutex_destroy(&mutex);
    
    printf("Результат: %lld! mod %lld = %lld\n", k, mod, result);
    
    return 0;
}