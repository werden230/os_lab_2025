CC = gcc
CFLAGS = -pthread

all: client server

client: client.o  utils.o
	$(CC) $(CFLAGS) -o client client.o  utils.o

server: server.o  utils.o
	$(CC) $(CFLAGS) -o server server.o  utils.o

server.o: server.c utils.h
	$(CC) $(CFLAGS) -c server.c

client.o: client.c utils.h
	$(CC) $(CFLAGS) -c client.c

utils.o: utils.c utils.h
	$(CC) $(CFLAGS) -c utils.c

clean:
	rm -f server client server.o client.o utils.o