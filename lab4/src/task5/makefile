CC = gcc
CFLAGS = -Wall -Wextra -pthread

all: parallel_sum

parallel_sum: parallel_sum.o sum_lib.o utils.o
	$(CC) $(CFLAGS) -o parallel_sum parallel_sum.o sum_lib.o utils.o

parallel_sum.o: parallel_sum.c sum_lib.h utils.h
	$(CC) $(CFLAGS) -c parallel_sum.c

sum_lib.o: sum_lib.c sum_lib.h
	$(CC) $(CFLAGS) -c sum_lib.c

utils.o: utils.c utils.h
	$(CC) $(CFLAGS) -c utils.c

clean:
	rm -f parallel_sum parallel_sum.o sum_lib.o utils.o
