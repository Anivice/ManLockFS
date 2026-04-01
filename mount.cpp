#include <cstdint>
# define FUSE_USE_VERSION 318
#include <fuse3/fuse.h>
#include <memory>
#include <sys/syscall.h>
#include <unistd.h>
#include <dirent.h>
#include <vector>
#include <sys/types.h>
#include <err.h>
#include <fcntl.h>
#include <unordered_map>
#include "cppcrc.h"
#include <sstream>
#include <algorithm>
#include <iostream>
#include <sys/ioctl.h>
#include <condition_variable>
#include <sys/vfs.h>
#include <cstring>
#include <filesystem>
namespace fs = std::filesystem;

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

std::string g_lock_fs_prefix;
std::string g_lock_fs_mount_point;
struct fd_lock_state_t {
    int lock_state;
};
std::mutex g_fd_lock_state_mutex;
std::unordered_map < uint64_t /* path hash */, fd_lock_state_t > g_fd_lock_state;
std::condition_variable g_cond_var;

#define STTR(X) #X
#define STR(x) STTR(x)

#define AUTO_ASSERT(x) if (!(x)) { throw std::runtime_error("Assertion failed! " #x " at " __FILE__ ":" STR(__LINE__)); }
#define ERROR_RPT                                                                       \
    catch (std::exception & e) {                                                        \
        std::cerr << "Error in " << __FUNCTION__ << ":" << e.what() << std::endl;       \
        return (errno == 0 ? -EIO : -errno);                                            \
    }                                                                                   \
    catch (...) {                                                                       \
        std::cerr << "Unknown error occurred!" << std::endl;                            \
        return (errno == 0 ? -EIO : -errno);                                            \
    }

void try_lock_and_block_when_trying(uint64_t ino, bool do_i_lock)
{
    std::unique_lock<std::mutex> lk(g_fd_lock_state_mutex);
    g_cond_var.wait(lk, [&] {
        const auto it = g_fd_lock_state.find(ino);
        return it == g_fd_lock_state.end()
            || it->second.lock_state != ML_LOCK_FILE;
    });

    if (do_i_lock) {
        g_fd_lock_state[ino] = { .lock_state = ML_LOCK_FILE };
    }
}

void unlock_by_checksum(const uint64_t checksum)
{
    std::lock_guard lock(g_fd_lock_state_mutex);
    g_fd_lock_state.erase(checksum);
    g_cond_var.notify_all();
}

uint64_t get_ino(const std::string & path)
{
    const int fd = open(path.c_str(), O_RDONLY);
    AUTO_ASSERT(fd >= 0);
    struct stat stbuf { };
    AUTO_ASSERT(fstat(fd, &stbuf) == 0);
    return stbuf.st_ino;
}

inline void lock_file(const std::string & path, const bool do_i_lock = false) {
    try_lock_and_block_when_trying(get_ino(path), do_i_lock);
}

inline void unlock_file(const std::string & path) {
    unlock_by_checksum(get_ino(path));
}

std::string replace_all(
    std::string & original,
    const std::string & target,
    const std::string & replacement) noexcept
{
    if (target.empty()) return original; // Avoid infinite loop if target is empty

    if (target.size() == 1 && replacement.empty()) {
        std::erase_if(original, [&target](const char c) { return c == target[0]; });
        return original;
    }

    size_t pos = 0;
    while ((pos = original.find(target, pos)) != std::string::npos) {
        original.replace(pos, target.length(), replacement);
        pos += replacement.length(); // Move past the replacement to avoid infinite loop
    }

    return original;
}

static int fuse_do_getattr(const char *path, struct stat *stbuf, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    if (!!stat(prefix.c_str(), stbuf)) {
        return -errno;
    }

    return 0;
}

struct linux_dirent {
    unsigned long  d_ino;
    off_t          d_off;
    unsigned short d_reclen;
    char           d_name[];
};

static int fuse_do_readdir(const char *path,
                           void *buffer,
                           const fuse_fill_dir_t filler,
                           off_t, fuse_file_info *, fuse_readdir_flags)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    std::vector < linux_dirent > dirents;
    const int fd = open(prefix.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd <= 0) return -errno;

    constexpr int BUF_SIZE = 4096;
    char buf [BUF_SIZE] = { };
    for (;;)
    {
        const long nread = syscall(SYS_getdents, fd, buf, BUF_SIZE);
        if (nread == -1)
            return -errno;

        if (nread == 0)
            break;

        for (size_t bpos = 0; bpos < nread;) {
            const auto * d = reinterpret_cast<linux_dirent *>(buf + bpos);
            filler(buffer, d->d_name, nullptr, 0, static_cast<fuse_fill_dir_flags>(0)); // FUSE_FILL_DIR_DEFAULTS = 0 not defined?
            bpos += d->d_reclen;
        }
    }

    filler(buffer, ".", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
    filler(buffer, "..", nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
    return 0;
}

static int fuse_do_mkdir(const char *path, const mode_t mode)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    return mkdir(prefix.c_str(), mode);
}

static int fuse_do_chmod(const char *path, const mode_t mode, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    return chmod(prefix.c_str(), mode);
}

static int fuse_do_chown(const char *path, const uid_t uid, const gid_t gid, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    return chown(prefix.c_str(), uid, gid);
}

static int fuse_do_create(const char *path, const mode_t mode, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    if (open(prefix.c_str(), O_CREAT | O_TRUNC, mode) == -1) {
        return -errno;
    }

    return 0;
}

static int fuse_do_flush(const char *, fuse_file_info *)
{
    return 0;
}

static int fuse_do_access(const char *path, const int mode)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    return access(prefix.c_str(), mode);
}

static int fuse_do_open(const char *path, fuse_file_info *fi)
{
    const std::string real = g_lock_fs_prefix + path;
    const int fd = open(real.c_str(), fi->flags);
    if (fd == -1) return -errno;
    fi->fh = static_cast<uint64_t>(fd);
    return 0;
}

static int fuse_do_read(const char *path, char *buf, size_t size, off_t off, fuse_file_info *fi)
{
    const std::string real = g_lock_fs_prefix + path;
    lock_file(real);
    const int fd = static_cast<int>(fi->fh);
    const ssize_t n = pread(fd, buf, size, off);
    return n == -1 ? -errno : static_cast<int>(n);
}

static int fuse_do_write(const char *path, const char *buf, size_t size, off_t off, fuse_file_info *fi)
{
    const std::string real = g_lock_fs_prefix + path;
    lock_file(real);
    const int fd = static_cast<int>(fi->fh);
    const ssize_t n = pwrite(fd, buf, size, off);
    return n == -1 ? -errno : static_cast<int>(n);
}

static int fuse_do_release(const char *, fuse_file_info *fi)
{
    return close(static_cast<int>(fi->fh)) == -1 ? -errno : 0;
}

static int fuse_do_utimens(const char *path, const timespec tv[2], fuse_file_info *fi)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    const auto fd = open(prefix.c_str(), O_RDWR);
    if (fd == -1) return -errno;
    return futimens(fd, tv);
}

static int fuse_do_unlink(const char *path)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    return unlink(prefix.c_str());
}

static int fuse_do_rmdir(const char *path)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    return rmdir(prefix.c_str());
}

static int fuse_do_fsync(const char *, int, fuse_file_info *) {
    return 0;
}

static int fuse_do_releasedir(const char *, fuse_file_info *) {
    return 0;
}

static int fuse_do_fsyncdir(const char *, int, fuse_file_info *) {
    return 0;
}

static int fuse_do_truncate(const char *path, const off_t size, fuse_file_info *) {
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    return truncate(prefix.c_str(), size);
}

static int fuse_do_symlink(const char *path, const char *target) {
    std::string prefix(g_lock_fs_prefix);
    prefix += target;
    lock_file(prefix);
    return symlink(prefix.c_str(), path);
}

static int fuse_do_ioctl(const char *, const unsigned int cmd, void *,
                         fuse_file_info *, const unsigned int flags, void *data)
{
    if (cmd == MLFS_LOCK)
    {
        try {
            const auto *msg = static_cast<lock_file_ioctl_msg_t *>(data);
            std::vector<char> buff(PATH_MAX, 0);
            std::string path = realpath(msg->lock_path, buff.data());
            replace_all(path, g_lock_fs_mount_point, g_lock_fs_prefix);
            const int fd = open(path.c_str(), O_RDWR, S_IREAD | S_IWRITE);
            AUTO_ASSERT(fd >= 0);
            struct stat stbuf { };
            AUTO_ASSERT(fstat(fd, &stbuf) == 0);

            if (msg->lock_action == ML_LOCK_FILE) {
                try_lock_and_block_when_trying(stbuf.st_ino, true /* actually lock */);
                return 0;
            } else if (msg->lock_action == ML_UNLOCK_FILE) {
                unlock_by_checksum(stbuf.st_ino);
                return 0;
            }
        }
        ERROR_RPT
    }

    return -EINVAL;
}

static int fuse_do_rename(const char *path, const char *name, unsigned int)
{
    std::string old_(g_lock_fs_prefix);
    std::string new_(g_lock_fs_prefix);
    old_ += path;
    new_ += name;
    lock_file(old_);
    lock_file(new_);
    return rename(old_.c_str(), new_.c_str());
}

static int fuse_do_fallocate(const char *path, const int mode, const off_t offset, const off_t length,
                             fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    const int fd = open(prefix.c_str(), O_RDWR | O_CREAT);
    if (fd <= 0) return -errno;
    return fallocate(fd, mode, offset, length);
}

static int fuse_do_readlink(const char *path, char *buffer, const size_t size)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    return readlink(prefix.c_str(), buffer, size);
}

void fuse_do_destroy(void *) {
}

void *fuse_do_init(fuse_conn_info *conn, fuse_config *) {
    return nullptr;
}

static int fuse_do_mknod(const char *path, const mode_t mode, const dev_t device)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    return mknod(prefix.c_str(), mode, device);
}

int fuse_statfs(const char * path, struct statvfs *status)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);
    struct statfs stat;
    const int ret = statfs(prefix.c_str(), &stat);
    if (ret == 0) {
        status->f_type = stat.f_type;
        status->f_bsize = stat.f_bsize;
        status->f_blocks = stat.f_blocks; 
        status->f_bfree = stat.f_bfree; 
        status->f_bavail = stat.f_bavail; 
        status->f_files = stat.f_files; 
        status->f_ffree = stat.f_ffree; 
        status->f_frsize = stat.f_frsize;
        return 0;
    }

    return ret;
}

static fuse_operations fuse_operation_vector_table {
    .getattr = fuse_do_getattr,
    .readlink = fuse_do_readlink,
    .mknod = fuse_do_mknod,
    .mkdir = fuse_do_mkdir,
    .unlink = fuse_do_unlink,
    .rmdir = fuse_do_rmdir,
    .symlink = fuse_do_symlink,
    .rename = fuse_do_rename,
    .chmod = fuse_do_chmod,
    .chown = fuse_do_chown,
    .truncate = fuse_do_truncate,
    .open = fuse_do_open,
    .read = fuse_do_read,
    .write = fuse_do_write,
    .statfs = fuse_statfs,
    .flush = fuse_do_flush,
    .release = fuse_do_release,
    .fsync = fuse_do_fsync,
    .opendir = fuse_do_open,
    .readdir = fuse_do_readdir,
    .releasedir = fuse_do_releasedir,
    .fsyncdir = fuse_do_fsyncdir,
    .init = fuse_do_init,
    .destroy = fuse_do_destroy,
    .access = fuse_do_access,
    .create = fuse_do_create,
    .utimens = fuse_do_utimens,
    .ioctl = fuse_do_ioctl,
    .fallocate = fuse_do_fallocate,
};

int fuse_redirect(const int argc, char ** argv)
{
    fuse_args args = FUSE_ARGS_INIT(argc, argv);
    if (fuse_opt_parse(&args, nullptr, nullptr, nullptr) == -1)
    {
        std::cerr << "FUSE initialization failed, errno: " << std::strerror(errno) << " (" << errno << ")\n";
        return EXIT_FAILURE;
    }

    int ret = 0;
    ret = fuse_main(args.argc, args.argv, &fuse_operation_vector_table, nullptr);
    fuse_opt_free_args(&args);
    return ret;
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        std::cout << *argv << " [Src.] [Mount Dest.]" << std::endl;
        return EXIT_FAILURE;
    }

    char buf [PATH_MAX] = { };
    g_lock_fs_prefix = realpath(argv[1], buf); // src
    g_lock_fs_mount_point = realpath(argv[2], buf); // dest
    std::unique_ptr<char*[]> fuse_argv;
    std::vector<std::string> fuse_args;
    if constexpr (DEBUG) {
        // fuse_args.emplace_back("-s");
        // fuse_args.emplace_back("-d");
        fuse_args.emplace_back("-f");
    }

    fuse_args.push_back(argv[2]);
    fuse_args.emplace_back("-o");
    fuse_args.emplace_back("subtype=mlfs");
    fuse_args.emplace_back("-o");
    fuse_args.emplace_back("fsname=" + std::string(argv[1]));

    fuse_argv = std::make_unique<char*[]>(fuse_args.size() + 1);
    fuse_argv[0] = argv[0]; // redirect
    for (int i = 0; i < static_cast<int>(fuse_args.size()); ++i) {
        fuse_argv[i + 1] = const_cast<char *>(fuse_args[i].c_str());
    }

    std::cout << "Mounting filesystem " << argv[1] << " to " << argv[2] << "\n";
    const int d_fuse_argc = static_cast<int>(fuse_args.size()) + 1;
    char ** d_fuse_argv = fuse_argv.get();
    return fuse_redirect(d_fuse_argc, d_fuse_argv);
}
