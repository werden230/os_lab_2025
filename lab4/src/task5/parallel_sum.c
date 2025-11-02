#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/time.h>

#include <pthread.h>

#include "sum_lib.h"
#include "utils.h"

int main(int argc, char **argv) {
    uint32_t threads_num = 0;
    uint32_t array_size = 0;
    uint32_t seed = 0;
    
    // Парсинг аргументов командной строки
    static struct option options[] = {
        {"threads_num", required_argument, 0, 0},
        {"array_size", required_argument, 0, 0},
        {"seed", required_argument, 0, 0},
        {0, 0, 0, 0}
    };

    int option_index = 0;
    while (true) {
        int c = getopt_long(argc, argv, "", options, &option_index);
        if (c == -1) break;

        switch (c) {
            case 0:
                switch (option_index) {
                    case 0:
                        threads_num = atoi(optarg);
                        if (threads_num <= 0) {
                            printf("Threads number must be positive\n");
                            return 1;
                        }
                        break;
                    case 1:
                        array_size = atoi(optarg);
                        if (array_size <= 0) {
                            printf("Array size must be positive\n");
                            return 1;
                        }
                        break;
                    case 2:
                        seed = atoi(optarg);
                        break;
                }
                break;
            case '?':
                break;
            default:
                printf("getopt returned character code 0%o?\n", c);
        }
    }

    if (threads_num == 0 || array_size == 0) {
        printf("Usage: %s --threads_num \"num\" --array_size \"num\" --seed \"num\"\n", argv[0]);
        return 1;
    }

    // Генерация массива (не входит в замер времени)
    int *array = malloc(sizeof(int) * array_size);
    GenerateArray(array, array_size, seed);

    // Подготовка аргументов для потоков
    struct SumArgs args[threads_num];
    int segment_size = array_size / threads_num;
    for (uint32_t i = 0; i < threads_num; i++) {
        args[i].array = array;
        args[i].begin = i * segment_size;
        args[i].end = (i == threads_num - 1) ? array_size : (i + 1) * segment_size;
    }

    // Создание потоков и замер времени
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);

    pthread_t threads[threads_num];
    
    // Создание потоков
    for (uint32_t i = 0; i < threads_num; i++) {
        if (pthread_create(&threads[i], NULL, ThreadSum, (void *)&args[i])) {
            printf("Error: pthread_create failed!\n");
            free(array);
            return 1;
        }
    }

    // Сбор результатов
    int total_sum = 0;
    for (uint32_t i = 0; i < threads_num; i++) {
        int sum = 0;
        pthread_join(threads[i], (void **)&sum);
        total_sum += sum;
    }

    gettimeofday(&end_time, NULL);

    // Вычисление времени выполнения
    double elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000.0;
    elapsed_time += (end_time.tv_usec - start_time.tv_usec) / 1000.0;

    // Вывод результатов
    printf("Total: %d\n", total_sum);
    printf("Elapsed time: %f ms\n", elapsed_time);

    free(array);
    return 0;
}