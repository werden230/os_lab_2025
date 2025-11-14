#ifndef UTILS_H
#define UTILS_H

#include <stdint.h>

struct FactorialArgs {
  uint64_t begin;
  uint64_t end;
  uint64_t mod;
};

struct Server {
  char ip[255];
  int port;
};

struct ThreadArgs {
  struct Server server;
  uint64_t begin;
  uint64_t end;
  uint64_t mod;
  uint64_t result;
};

uint64_t MultModulo(uint64_t a, uint64_t b, uint64_t mod);

#endif