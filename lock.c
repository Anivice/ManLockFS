#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <linux/limits.h>
#include <sys/stat.h>

struct lock_file_ioctl_msg_t {
    uint64_t lock_action;
    uint64_t locker_ino;
    uint64_t content_length;
    char lock_path[256];
    char content_data[512];
};

#define MLFS_LOCK   _IOW('M', 0x42, struct lock_file_ioctl_msg_t)

#define ML_LOCK_FILE      (1)
#define ML_UNLOCK_FILE    (2)
#define ML_APPEND_FILE    (3)

#define min(a, b) (((a) < (b)) ? (a) : (b))
const char * actions[] = { "lock", "unlock", "append" };

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s <target file> <action:lock/unlock/append>\n", argv[0]);
        return EXIT_FAILURE;
    }

    auto const target_file = argv[1];
    auto const action = argv[2];

    const int fd = open(target_file, O_RDONLY | O_CREAT);
    if (fd == -1) {
        perror("open");
        return EXIT_FAILURE;
    }

    uint64_t action_determined = UINT64_MAX;
    for (uint64_t i = 0; i < sizeof(actions) / sizeof(actions[0]); i++) {
        if (!strncmp(actions[i], action, min(sizeof(action), strlen(actions[i])))) {
            action_determined = i;
        }
    }

    if (action_determined == UINT64_MAX) {
        fprintf(stderr, "%s is not a valid action\n", action);
        return EXIT_FAILURE;
    }

    struct lock_file_ioctl_msg_t irq = {};
    char abs_path [PATH_MAX] = { };
    if (realpath(target_file, abs_path) == nullptr) {
        perror("realpath");
        return EXIT_FAILURE;
    }

    struct stat stbuf = {};
    if (fstat(fd, &stbuf) == -1) {
        perror("fstat");
        return EXIT_FAILURE;
    }

    switch (action_determined)
    {
        case 0: // lock
        case 1: // unlock
        {
            strncpy(irq.lock_path, abs_path, sizeof(irq.lock_path));
            irq.lock_action = (action_determined == 0 ? ML_LOCK_FILE : ML_UNLOCK_FILE);
            irq.locker_ino = stbuf.st_ino;
            if (ioctl(fd, MLFS_LOCK, &irq) == -1) {
                perror("ioctl");
                return EXIT_FAILURE;
            }
        }
            break;

        case 2: // append
        {
            strncpy(irq.lock_path, abs_path, sizeof(irq.lock_path));
            irq.locker_ino = stbuf.st_ino;
            irq.lock_action = ML_APPEND_FILE;
            irq.content_length = read(STDIN_FILENO, irq.content_data, sizeof(irq.content_data));
            if (ioctl(fd, MLFS_LOCK, &irq) == -1) {
                perror("ioctl");
                return EXIT_FAILURE;
            }
        }
            break;

        default: break;
    }

    return EXIT_SUCCESS;
}