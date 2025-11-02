#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char const *argv[])
{
	pid_t ret;

	ret = fork();

	if (ret == -1)
	{
		perror("fork failed");
		exit(-1);
	}

	if (ret == 0)
	{
		printf("I'am a child\n");
		printf("My pid %d\n", getpid());
		printf("My ppid %d\n", getppid());
		exit(0);
	}
	else
	{
		printf("I'am a parent\n");
		printf("My pid %d\n", getpid());
		printf("My ppid %d\n", getppid());
		sleep(60);
		exit(0);
	}
	return 0;
}