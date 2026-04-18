/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */
 
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"

static char *alloc_stack() {
    char *stack = malloc(STACK_SIZE);
    return stack ? stack + STACK_SIZE : NULL;
}

/* ---------------- DATA STRUCTURES (unchanged) ---------------- */
#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    STATE_RUNNING,
    STATE_EXITED,
    STATE_KILLED
} runtime_state_t;

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
    int log_read_fd;
} child_config_t;

typedef struct runtime_container {
    char id[CONTAINER_ID_LEN];
    pid_t pid;
    int log_fd;

    int monitor_fd;

    runtime_state_t state;
    int exit_code;
    int exit_signal;

    struct runtime_container *next;
} runtime_container_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    runtime_container_t *running;
} supervisor_ctx_t;

typedef struct {
    int fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} log_reader_arg_t;

typedef struct {
    pid_t pid;
    int log_fd;
} launch_result_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/* ---------------- BOUNDED BUFFER ---------------- */

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ---------------- LOGGING THREAD ---------------- */

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    mkdir(LOG_DIR, 0755);

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        FILE *fp = fopen(path, "a");
        if (!fp) continue;

        fwrite(item.data, 1, item.length, fp);
        fclose(fp);
    }
    return NULL;
}

/* ---------------- CONTAINER ENTRY ---------------- */

static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* child does not read from pipe */
    close(cfg->log_read_fd);

    /* 1. isolate mount namespace */
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0) {
        perror("mount MS_PRIVATE");
        exit(1);
    }

    /* 2. switch rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        exit(1);
    }

    if (chdir("/") != 0) {
        perror("chdir");
        exit(1);
    }

    /* 3. mount /proc */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount proc");
        exit(1);
    }

    /* 4. hostname */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        exit(1);
    }

    /* 5. redirect stdout + stderr to pipe */
    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    /* 6. execute command */
    execl("/bin/sh", "sh", "-c", cfg->command, NULL);

    /* only reached if exec fails */
    perror("exec failed");
    exit(1);
}

void *pipe_reader_thread(void *arg)
{
    log_reader_arg_t *ctx = (log_reader_arg_t *)arg;

    char buf[LOG_CHUNK_SIZE];

    while (1) {
        ssize_t n = read(ctx->fd, buf, sizeof(buf) - 1);
        if (n <= 0) break;

        buf[n] = '\0';

        log_item_t item;
        memset(&item, 0, sizeof(item));

        strncpy(item.container_id, ctx->container_id, CONTAINER_ID_LEN);
        item.length = n;
        memcpy(item.data, buf, n);

        bounded_buffer_push(ctx->buffer, &item);
    }

	//printf("READ %ld bytes from %s\n", n, ctx->container_id);
    close(ctx->fd);
    return NULL;
}

/* ---------------- CLONE LAUNCH ---------------- */

static launch_result_t launch_container(child_config_t *cfg)
{
    launch_result_t res;
    res.pid = -1;
    res.log_fd = -1;

    char *stack = mmap(NULL, STACK_SIZE,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS,
                   -1, 0);

	if (stack == MAP_FAILED) {
	    perror("mmap stack");
	    return res;
	}
    
    void *stack_top = stack + STACK_SIZE;

    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        free(stack);
        return res;
    }

    cfg->log_write_fd = pipefd[1];
    cfg->log_read_fd  = pipefd[0];

    int flags =
	    CLONE_NEWPID |
	    CLONE_NEWUTS |
	    CLONE_NEWNS |
	    SIGCHLD;

    res.pid = clone(child_fn, stack + STACK_SIZE, flags, cfg);
    if (res.pid < 0) {
        perror("clone");
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return res;
    }

    close(pipefd[1]);
    res.log_fd = pipefd[0];

    /* IMPORTANT: we intentionally leak stack here for assignment simplicity */
    return res;
}

/* ---------------- CONTROL SOCKET ---------------- */

static int setup_server_socket()
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    unlink(CONTROL_PATH);
    bind(fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(fd, 10);

    return fd;
}

/* ---------------- SUPERVISOR ---------------- */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {

        pthread_mutex_lock(&ctx->metadata_lock);

        runtime_container_t *cur = ctx->running;

        while (cur) {
            if (cur->pid == pid) {

                if (WIFEXITED(status)) {
                    cur->state = STATE_EXITED;
                    cur->exit_code = WEXITSTATUS(status);
                    printf("Container %s exited (code=%d)\n",
                           cur->id, cur->exit_code);
                }
                else if (WIFSIGNALED(status)) {
                    cur->state = STATE_KILLED;
                    cur->exit_signal = WTERMSIG(status);
                    printf("Container %s killed (signal=%d)\n",
                           cur->id, cur->exit_signal);
                }

                break;
            }
            cur = cur->next;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

const char *state_str(runtime_state_t s)
{
    switch (s) {
        case STATE_RUNNING: return "RUNNING";
        case STATE_EXITED:  return "EXITED";
        case STATE_KILLED:  return "KILLED";
        default: return "UNKNOWN";
    }
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    ctx.server_fd = setup_server_socket();
    bounded_buffer_init(&ctx.log_buffer);
    pthread_mutex_init(&ctx.metadata_lock, NULL);

    pthread_t logger;
    pthread_create(&logger, NULL, logging_thread, &ctx);

    printf("Supervisor running with rootfs: %s\n", rootfs);

    while (1) {

        /* ---------------- REAP CHILDREN ---------------- */
        int status;
        pid_t pid;

        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {

            pthread_mutex_lock(&ctx.metadata_lock);

            runtime_container_t *cur = ctx.running;

            while (cur) {
                if (cur->pid == pid) {

                    if (WIFEXITED(status)) {
                        cur->state = STATE_EXITED;
                        cur->exit_code = WEXITSTATUS(status);
                        printf("Container %s exited (code=%d)\n",
                               cur->id, cur->exit_code);
                    }
                    else if (WIFSIGNALED(status)) {
                        cur->state = STATE_KILLED;
                        cur->exit_signal = WTERMSIG(status);
                        printf("Container %s killed (signal=%d)\n",
                               cur->id, cur->exit_signal);
                    }

                    break;
                }
                cur = cur->next;
            }

            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        /* ---------------- HANDLE CLIENT ---------------- */
        int client = accept(ctx.server_fd, NULL, NULL);

        control_request_t req;
        read(client, &req, sizeof(req));

        /* ---------------- START ---------------- */
        if (req.kind == CMD_START || req.kind == CMD_RUN) {

            /* clear old logs */
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);
            FILE *fp = fopen(path, "w");
            if (fp) fclose(fp);

            child_config_t *cfg = malloc(sizeof(child_config_t));
            memset(cfg, 0, sizeof(*cfg));

            strcpy(cfg->id, req.container_id);
            strcpy(cfg->rootfs, req.rootfs);
            strcpy(cfg->command, req.command);

            launch_result_t res = launch_container(cfg);

            /* -------- REGISTER WITH KERNEL MODULE -------- */
            int mon_fd = open("/dev/container_monitor", O_RDWR);
            if (mon_fd < 0) {
                perror("open /dev/container_monitor");
            } else {
                struct monitor_request mreq;
                memset(&mreq, 0, sizeof(mreq));

                mreq.pid = res.pid;
                mreq.soft_limit_bytes = req.soft_limit_bytes;
                mreq.hard_limit_bytes = req.hard_limit_bytes;
                strncpy(mreq.container_id, cfg->id, MONITOR_NAME_LEN);

                if (ioctl(mon_fd, MONITOR_REGISTER, &mreq) < 0) {
                    perror("ioctl REGISTER");
                }
            }

            /* -------- TRACK CONTAINER -------- */
            runtime_container_t *node = malloc(sizeof(*node));
            memset(node, 0, sizeof(*node));

            strcpy(node->id, cfg->id);
            node->pid = res.pid;
            node->log_fd = res.log_fd;
            node->monitor_fd = mon_fd;

            node->state = STATE_RUNNING;
            node->exit_code = -1;
            node->exit_signal = 0;

            pthread_mutex_lock(&ctx.metadata_lock);
            node->next = ctx.running;
            ctx.running = node;
            pthread_mutex_unlock(&ctx.metadata_lock);

            /* -------- PIPE READER THREAD -------- */
            log_reader_arg_t *lr = malloc(sizeof(log_reader_arg_t));
            lr->fd = res.log_fd;
            strcpy(lr->container_id, cfg->id);
            lr->buffer = &ctx.log_buffer;

            pthread_t tid;
            pthread_create(&tid, NULL, pipe_reader_thread, lr);
            pthread_detach(tid);

            printf("Started container %s PID %d\n", cfg->id, res.pid);
        }

        /* ---------------- STOP ---------------- */
        else if (req.kind == CMD_STOP) {

            pthread_mutex_lock(&ctx.metadata_lock);

            runtime_container_t *cur = ctx.running;

            while (cur) {
                if (strcmp(cur->id, req.container_id) == 0) {

                    /* -------- UNREGISTER FIRST -------- */
                    if (cur->monitor_fd >= 0) {
                        struct monitor_request mreq;
                        memset(&mreq, 0, sizeof(mreq));

                        mreq.pid = cur->pid;
                        strncpy(mreq.container_id, cur->id, MONITOR_NAME_LEN);

                        if (ioctl(cur->monitor_fd, MONITOR_UNREGISTER, &mreq) < 0) {
			    if (errno != ENOENT) {
				perror("ioctl UNREGISTER");
			    }
			}

                        close(cur->monitor_fd);
                        cur->monitor_fd = -1;
                    }

                    /* -------- TERMINATE -------- */
                    if (cur->state == STATE_RUNNING) {
			    kill(cur->pid, SIGTERM);
			}

                    printf("Sent SIGTERM to %s (PID %d)\n",
                           cur->id, cur->pid);

                    break;
                }
                cur = cur->next;
            }

            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        /* ---------------- PS ---------------- */
        else if (req.kind == CMD_PS) {

            pthread_mutex_lock(&ctx.metadata_lock);

            printf("==== Containers ====\n");

            runtime_container_t *cur = ctx.running;

            if (!cur) {
                printf("(none)\n");
            } else {
                while (cur) {

                    const char *state =
                        (cur->state == STATE_RUNNING) ? "RUNNING" :
                        (cur->state == STATE_EXITED)  ? "EXITED"  :
                        (cur->state == STATE_KILLED)  ? "KILLED"  : "UNKNOWN";

                    printf("ID: %s | PID: %d | STATE: %s\n",
                           cur->id, cur->pid, state);

                    cur = cur->next;
                }
            }

            printf("====================\n");

            pthread_mutex_unlock(&ctx.metadata_lock);
        }

        /* ---------------- LOGS ---------------- */
        else if (req.kind == CMD_LOGS) {

            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req.container_id);

            FILE *fp = fopen(path, "r");
            if (!fp) {
                perror("open log file");
            } else {
                char line[512];
                while (fgets(line, sizeof(line), fp)) {
                    printf("%s", line);
                }
                fclose(fp);
            }
        }

        close(client);
    }

    /* not reached, but correct cleanup */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(logger, NULL);

    return 0;
}

/* ---------------- CLIENT ---------------- */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        return 1;
    }

    write(fd, req, sizeof(*req));
    close(fd);
    return 0;
}

/* ---------------- CLI WRAPPERS (unchanged) ---------------- */
/* cmd_start, cmd_run, cmd_stop, etc remain same */


static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    printf("Check supervisor terminal for container list\n");

    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "logs/%s.log", argv[2]);

    FILE *fp = fopen(path, "r");
    if (!fp) {
        perror("open log file");
        return 1;
    }

    char buf[1024];
    while (fgets(buf, sizeof(buf), fp)) {
        printf("%s", buf);
    }

    fclose(fp);
    return 0;
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}


int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
