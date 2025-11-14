#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <errno.h>
#include <getopt.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>

#include "utils.h"

bool ConvertStringToUI64(const char *str, uint64_t *val) {
  char *end = NULL;
  unsigned long long i = strtoull(str, &end, 10);
  if (errno == ERANGE) {
    fprintf(stderr, "Out of uint64_t range: %s\n", str);
    return false;
  }

  if (errno != 0)
    return false;

  *val = i;
  return true;
}

int ReadServersFromFile(const char *filename, struct Server **servers) {
  FILE *file = fopen(filename, "r");
  if (file == NULL) {
    fprintf(stderr, "Cannot open servers file: %s\n", filename);
    return -1;
  }

  int capacity = 10;
  int count = 0;
  *servers = malloc(sizeof(struct Server) * capacity);

  char line[512];
  while (fgets(line, sizeof(line), file)) {
    line[strcspn(line, "\n")] = '\0';
    
    if (strlen(line) == 0 || line[0] == '#')
      continue;

    char *colon = strchr(line, ':');
    if (colon == NULL) {
      fprintf(stderr, "Invalid server format: %s (expected ip:port)\n", line);
      continue;
    }

    *colon = '\0';
    char *ip = line;
    int port = atoi(colon + 1);

    if (port <= 0 || port > 65535) {
      fprintf(stderr, "Invalid port in server: %s:%d\n", ip, port);
      continue;
    }

    if (count >= capacity) {
      capacity *= 2;
      *servers = realloc(*servers, sizeof(struct Server) * capacity);
    }

    strncpy((*servers)[count].ip, ip, sizeof((*servers)[count].ip) - 1);
    (*servers)[count].ip[sizeof((*servers)[count].ip) - 1] = '\0';
    (*servers)[count].port = port;
    count++;
  }

  fclose(file);
  return count;
}

void *ProcessServer(void *args) {
  struct ThreadArgs *thread_args = (struct ThreadArgs *)args;
  
  struct hostent *hostname = gethostbyname(thread_args->server.ip);
  if (hostname == NULL) {
    fprintf(stderr, "gethostbyname failed with %s\n", thread_args->server.ip);
    thread_args->result = 1;
    pthread_exit(NULL);
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(thread_args->server.port);
  server_addr.sin_addr.s_addr = *((unsigned long *)hostname->h_addr);

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    fprintf(stderr, "Socket creation failed for server %s!\n", thread_args->server.ip);
    thread_args->result = 1;
    pthread_exit(NULL);
  }

  struct timeval timeout;
  timeout.tv_sec = 5;
  timeout.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    fprintf(stderr, "Connection failed to server %s:%d\n", 
            thread_args->server.ip, thread_args->server.port);
    close(sock);
    thread_args->result = 1;
    pthread_exit(NULL);
  }

  char task[sizeof(uint64_t) * 3];
  memcpy(task, &thread_args->begin, sizeof(uint64_t));
  memcpy(task + sizeof(uint64_t), &thread_args->end, sizeof(uint64_t));
  memcpy(task + 2 * sizeof(uint64_t), &thread_args->mod, sizeof(uint64_t));

  if (send(sock, task, sizeof(task), 0) < 0) {
    fprintf(stderr, "Send failed to server %s\n", thread_args->server.ip);
    close(sock);
    thread_args->result = 1;
    pthread_exit(NULL);
  }

  char response[sizeof(uint64_t)];
  if (recv(sock, response, sizeof(response), 0) < 0) {
    fprintf(stderr, "Receive failed from server %s\n", thread_args->server.ip);
    close(sock);
    thread_args->result = 1;
    pthread_exit(NULL);
  }

  memcpy(&thread_args->result, response, sizeof(uint64_t));
  close(sock);
  
  printf("Server %s:%d returned result: %llu for range [%llu, %llu]\n",
         thread_args->server.ip, thread_args->server.port,
         thread_args->result, thread_args->begin, thread_args->end);
  
  pthread_exit(NULL);
}

int main(int argc, char **argv) {
  uint64_t k = -1;
  uint64_t mod = -1;
  char servers_file[255] = {'\0'};

  while (true) {
    int current_optind = optind ? optind : 1;

    static struct option options[] = {{"k", required_argument, 0, 0},
                                      {"mod", required_argument, 0, 0},
                                      {"servers", required_argument, 0, 0},
                                      {0, 0, 0, 0}};

    int option_index = 0;
    int c = getopt_long(argc, argv, "", options, &option_index);

    if (c == -1)
      break;

    switch (c) {
    case 0: {
      switch (option_index) {
      case 0:
        if (!ConvertStringToUI64(optarg, &k)) {
          fprintf(stderr, "Error: Invalid value for k '%s'. Must be a positive integer.\n", optarg);
          return 1;
        }
        if (k == 0) {
          fprintf(stderr, "Error: k must be greater than 0.\n");
          return 1;
        }
        printf("k = %llu\n", k);
        break;
      case 1:
        if (!ConvertStringToUI64(optarg, &mod)) {
          fprintf(stderr, "Error: Invalid value for mod '%s'. Must be a positive integer.\n", optarg);
          return 1;
        }
        if (mod == 0) {
          fprintf(stderr, "Error: mod must be greater than 0.\n");
          return 1;
        }
        printf("mod = %llu\n", mod);
        break;
      case 2:
        if (strlen(optarg) == 0) {
          fprintf(stderr, "Error: Servers file path cannot be empty.\n");
          return 1;
        }
        if (strlen(optarg) >= sizeof(servers_file)) {
          fprintf(stderr, "Error: Servers file path too long.\n");
          return 1;
        }
        strncpy(servers_file, optarg, sizeof(servers_file) - 1);
        servers_file[sizeof(servers_file) - 1] = '\0';
        printf("servers file = %s\n", servers_file);
        break;
      default:
        printf("Index %d is out of options\n", option_index);
        return 1;
      }
    } break;

    case '?':
      fprintf(stderr, "Arguments error\n");
      fprintf(stderr, "Using: %s --k 1000 --mod 5 --servers /path/to/file\n", argv[0]);
      return 1;
      break;
    default:
      fprintf(stderr, "getopt returned character code 0%o?\n", c);
      return 1;
    }
  }

  if (optind < argc) {
    fprintf(stderr, "Error: Unexpected arguments: ");
    for (int i = optind; i < argc; i++) {
      fprintf(stderr, "%s ", argv[i]);
    }
    fprintf(stderr, "\n");
    return 1;
  }

  if (k == -1 || mod == -1 || !strlen(servers_file)) {
    fprintf(stderr, "Using: %s --k 1000 --mod 5 --servers /path/to/file\n", argv[0]);
    return 1;
  }

  struct Server *servers = NULL;
  unsigned int servers_num = ReadServersFromFile(servers_file, &servers);
  if (servers_num <= 0) {
    fprintf(stderr, "Error: No valid servers found in file %s\n", servers_file);
    return 1;
  }

  printf("Found %d servers\n", servers_num);

  
  pthread_t threads[servers_num];
  struct ThreadArgs thread_args[servers_num];

  uint64_t range = k / servers_num;
  uint64_t remainder = k % servers_num;
  uint64_t current = 1;

  for (int i = 0; i < servers_num; i++) {
    thread_args[i].server = servers[i];
    thread_args[i].begin = current;
    thread_args[i].end = current + range - 1;
    thread_args[i].mod = mod;
    
    if (remainder > 0) {
      thread_args[i].end++;
      remainder--;
    }
    
    current = thread_args[i].end + 1;
    
    printf("Server %d (%s:%d) will process range [%llu, %llu]\n",
           i, servers[i].ip, servers[i].port, 
           thread_args[i].begin, thread_args[i].end);

    if (pthread_create(&threads[i], NULL, ProcessServer, &thread_args[i]) != 0) {
      fprintf(stderr, "Error creating thread for server %s\n", servers[i].ip);
    }
  }

  for (int i = 0; i < servers_num; i++) {
    pthread_join(threads[i], NULL);
  }

  uint64_t total = 1;
  for (int i = 0; i < servers_num; i++) {
    if (thread_args[i].result != 1) {
      total = MultModulo(total, thread_args[i].result, mod);
    } else {
      printf("Warning: Server %s:%d failed, skipping its result\n",
             servers[i].ip, servers[i].port);
    }
  }

  printf("Final answer: %llu! mod %llu = %llu\n", k, mod, total);

  free(servers);
  return 0;
}