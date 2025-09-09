#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>

int main() {
    pid_t pid = fork();

    if (pid == 0) {
        printf("Child PID: %d\n", getpid());
        execl("./seq", "seq", "13", "10000000");

        perror("execl failed");
        return 1;

    } else if (pid > 0) {
        printf("Parent PID: %d\n", getpid());
        wait(NULL);
        printf("Дочерний процесс завершился.\n");

    } else {
        perror("fork failed");
        return 1;
    }
    return 0;
}
