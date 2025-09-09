#include <ctype.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <getopt.h>

#include "find_min_max.h"
#include "utils.h"

int main(int argc, char **argv) {
    int seed = -1;
    int array_size = -1;
    int pnum = -1;
    bool with_files = false;

    while (true) {
        int current_optind = optind ? optind : 1;

        static struct option options[] = {{"seed", required_argument, 0, 0},
                                          {"array_size", required_argument, 0, 0},
                                          {"pnum", required_argument, 0, 0},
                                          {"by_files", no_argument, 0, 'f'},
                                          {0, 0, 0, 0}};

        int option_index = 0;
        int c = getopt_long(argc, argv, "f", options, &option_index);

        if (c == -1) break;

        switch (c) {
            case 0:
                switch (option_index) {
                    case 0:
                        seed = atoi(optarg);
                        if (seed <= 0) {
                            printf("Seed must be a positive number\n");
                            return 1;
                        }
                        break;
                    case 1:
                        array_size = atoi(optarg);
                        if (array_size <= 0) {
                            printf("Array size must be a positive number\n");
                            return 1;
                        }
                        break;
                    case 2:
                        pnum = atoi(optarg);
                        if (pnum <= 0) {
                            printf("Process number must be a positive number\n");
                            return 1;
                        }
                        break;
                    case 3:
                        with_files = true;
                        break;

                    default:
                        printf("Index %d is out of options\n", option_index);
                }
                break;
            case 'f':
                with_files = true;
                break;

            case '?':
                break;

            default:
                printf("getopt returned character code 0%o?\n", c);
        }
    }

    if (optind < argc) {
        printf("Has at least one no option argument\n");
        return 1;
    }

    if (seed == -1 || array_size == -1 || pnum == -1) {
        printf("Usage: %s --seed \"num\" --array_size \"num\" --pnum \"num\" [--by_files]\n",
               argv[0]);
        return 1;
    }

    int *array = malloc(sizeof(int) * array_size);
    GenerateArray(array, array_size, seed);
    int active_child_processes = 0;

    // Создаем пайпы или файлы 
    int *pipefds = NULL;
    char **filenames = NULL;
    
    if (!with_files) {
        pipefds = malloc(2 * pnum * sizeof(int));
        for (int i = 0; i < pnum; i++) {
            if (pipe(pipefds + 2*i) == -1) {
                perror("pipe failed");
                return 1;
            }
        }
    } else {
        filenames = malloc(pnum * sizeof(char*));
        for (int i = 0; i < pnum; i++) {
            filenames[i] = malloc(20 * sizeof(char));
            snprintf(filenames[i], 20, "result_%d.txt", i);
        }
    }

    struct timeval start_time;
    gettimeofday(&start_time, NULL);

    // Создаем дочерние процессы
    for (int i = 0; i < pnum; i++) {
        pid_t child_pid = fork();
        if (child_pid >= 0) {
            // successful fork
            active_child_processes += 1;
            if (child_pid == 0) {
                // child process
                
                int chunk_size = array_size / pnum;
                int start = i * chunk_size;
                int end = (i == pnum - 1) ? array_size : start + chunk_size;
                
                int local_min = INT_MAX;
                int local_max = INT_MIN;
                
                for (int j = start; j < end; j++) {
                    if (array[j] < local_min) local_min = array[j];
                    if (array[j] > local_max) local_max = array[j];
                }
                
                if (with_files) {
                    FILE *file = fopen(filenames[i], "w");
                    if (file) {
                        fprintf(file, "%d %d", local_min, local_max);
                        fclose(file);
                    }
                } else {
                    close(pipefds[2*i]);
                    FILE *stream = fdopen(pipefds[2*i + 1], "w");
                    if (stream) {
                        fprintf(stream, "%d %d", local_min, local_max);
                        fclose(stream);
                    }
                    close(pipefds[2*i + 1]);
                }
                
                free(array);
                if (pipefds) free(pipefds);
                if (filenames) {
                    for (int j = 0; j < pnum; j++) free(filenames[j]);
                    free(filenames);
                }
                
                return 0;
            }
        } else {
            printf("Fork failed!\n");
            return 1;
        }
    }

    while (active_child_processes > 0) {
	  int status;
	  pid_t finished_pid = wait(&status);  
	  if (finished_pid == -1) {
	    perror("wait failed");
	    break;
	  }
	  active_child_processes -= 1;
    }
	
    struct MinMax min_max;
    min_max.min = INT_MAX;
    min_max.max = INT_MIN;

    for (int i = 0; i < pnum; i++) {
        int min = INT_MAX;
        int max = INT_MIN;

        if (with_files) {
            FILE *file = fopen(filenames[i], "r");
            if (file) {
                fscanf(file, "%d %d", &min, &max);
                fclose(file);
                remove(filenames[i]);
            }
        } else {
            close(pipefds[2*i + 1]);
            FILE *stream = fdopen(pipefds[2*i], "r");
            if (stream) {
                fscanf(stream, "%d %d", &min, &max);
                fclose(stream);
            }
            close(pipefds[2*i]);
        }

        if (min < min_max.min) min_max.min = min;
        if (max > min_max.max) min_max.max = max;
    }

    struct timeval finish_time;
    gettimeofday(&finish_time, NULL);

    double elapsed_time = (finish_time.tv_sec - start_time.tv_sec) * 1000.0;
    elapsed_time += (finish_time.tv_usec - start_time.tv_usec) / 1000.0;

    free(array);

    printf("Min: %d\n", min_max.min);
    printf("Max: %d\n", min_max.max);
    printf("Elapsed time: %fms\n", elapsed_time);
    fflush(NULL);
    return 0;
}
