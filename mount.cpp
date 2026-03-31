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

extern "C" struct lock_file_ioctl_msg {
    uint64_t lock_action;
    uint64_t locker_pid;
    uint64_t content_length;
    char lock_path[256];
    char content_data[512];
};

#define MLFS_LOCK   _IOW('M', 0x42, struct lock_file_ioctl_msg)

#define ML_LOCK_FILE      (1)
#define ML_UNLOCK_FILE    (2)
#define ML_APPEND_FILE    (3)

std::string g_lock_fs_prefix;
struct fd_status_t {
    int fd;
};

struct fd_lock_state_t {
    int lock_state;
};
std::mutex g_fd_tree_mutex;
std::mutex g_fd_lock_state_mutex;
std::unordered_map < uint64_t /* path hash */, fd_status_t > g_fd_tree;
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

void try_lock_and_block_when_trying(const uint64_t checksum,
    const bool do_i_lock)
{
    auto try_acquire_lock = [&]->bool
    {
        std::lock_guard lock(g_fd_lock_state_mutex);
        if (const auto it = g_fd_lock_state.find(checksum);
            it != g_fd_lock_state.end() && it->second.lock_state == ML_LOCK_FILE) {
            return false;
        }

        if (do_i_lock) g_fd_lock_state[checksum] = {
                .lock_state  = ML_LOCK_FILE,
            };
        return true;
    };

    while (!try_acquire_lock())
    {
        std::unique_lock<std::mutex> lock(g_fd_lock_state_mutex);
        g_cond_var.wait(lock, [&]->bool
        {
            const auto it = g_fd_lock_state.find(checksum);
            // exist and locked, or doesn't exist at all
            return ((it != g_fd_lock_state.end() && it->second.lock_state == ML_LOCK_FILE) || it == g_fd_lock_state.end());
        });
    }
}

void unlock_by_checksum(const uint64_t checksum)
{
    std::lock_guard lock(g_fd_lock_state_mutex);
    g_fd_lock_state.erase(checksum);
    g_cond_var.notify_all();
}

inline void lock_file(const std::string & path, const bool do_i_lock = false)
{
    try_lock_and_block_when_trying(
        CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(path.c_str()), path.length()),
        do_i_lock);
}

inline void unlock_file(const std::string & path) {
    unlock_by_checksum(CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(path.c_str()), path.length()));
}

std::string path_calculator(const std::string &path) noexcept
{
    const fs::path p {path};
    const fs::path n = p.lexically_normal();
    return n.generic_string();
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

static int fuse_do_flush(const char * path, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;

    {
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        if (const auto it = g_fd_tree.find(path_crc64); it != g_fd_tree.end()) {
            return fsync(it->second.fd);
        }
    }

    return 0;
}

static int fuse_do_release(const char *path, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;

    {
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        if (const auto it = g_fd_tree.find(path_crc64); it != g_fd_tree.end()) {
            const int fd = it->second.fd;
            g_fd_tree.erase(it);
            return close(fd);
        }
    }

    return 0;
}

static int fuse_do_access(const char *path, const int mode)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    int status = access(prefix.c_str(), mode);
    return status;
}

static int fuse_do_open(const char *path, fuse_file_info * info)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);

    const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
    std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
    if (const auto it = g_fd_tree.find(path_crc64); it == g_fd_tree.end()) {
        if (const auto fd = open(prefix.c_str(), info->flags); fd > 0) {
            g_fd_tree[path_crc64] = { .fd = fd };
            return 0;
        }

        return (errno == 0 ? -EIO : -errno);
    }

    return 0;
}

static int fuse_do_read(const char *path, char *buffer, const size_t size, const off_t offset, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);

    try {
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        const auto fd = g_fd_tree.at(path_crc64).fd;
        AUTO_ASSERT(lseek(fd, offset, SEEK_SET) == 0);
        return static_cast<int>(read(fd, buffer, size));
    }
    ERROR_RPT
}

static int fuse_do_write(const char *path, const char *buffer, const size_t size, const off_t offset,
                         fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);

    try {
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        const auto fd = g_fd_tree.at(path_crc64).fd;
        AUTO_ASSERT(lseek(fd, offset, SEEK_SET) == 0);
        return static_cast<int>(write(fd, buffer, size));
    }
    ERROR_RPT
}

static int fuse_do_utimens(const char *path, const timespec tv[2], fuse_file_info *fi)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);

    try {
        fuse_file_info flag { .flags = O_RDWR };
        fuse_do_open(path, &flag);
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        const auto fd = g_fd_tree.at(path_crc64).fd;
        return futimens(fd, tv);
    }
    ERROR_RPT
}

static int fuse_do_unlink(const char *path)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    lock_file(prefix);

    try {
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        if (const auto it = g_fd_tree.find(path_crc64); it != g_fd_tree.end()) {
            close(it->second.fd);
            g_fd_tree.erase(it);
        }

        return unlink(prefix.c_str());
    }
    ERROR_RPT
}

static int fuse_do_rmdir(const char *path)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    return rmdir(prefix.c_str());
}

static int fuse_do_fsync(const char *path, int, fuse_file_info *)
{
    std::string prefix(g_lock_fs_prefix);
    prefix += path;
    try {
        const uint64_t path_crc64 = CRC64::ECMA::calc(reinterpret_cast<const uint8_t *>(prefix.c_str()), prefix.length());
        std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
        if (const auto it = g_fd_tree.find(path_crc64); it != g_fd_tree.end()) {
            return fsync(it->second.fd);
        }

        return 0;
    }
    ERROR_RPT
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
        const auto *msg = static_cast<lock_file_ioctl_msg *>(data);
        std::string prefix(g_lock_fs_prefix);
        prefix += "/";
        prefix += msg->lock_path;
        prefix = path_calculator(prefix);
        if (msg->lock_action == ML_LOCK_FILE) {
            lock_file(prefix, true /* actually lock */);
            return 0;
        } else if (msg->lock_action == ML_UNLOCK_FILE) {
            unlock_file(prefix);
            return 0;
        } else if (msg->lock_action == ML_APPEND_FILE) {
            const uint64_t path_crc64 = CRC64::ECMA::calc(
                reinterpret_cast<const uint8_t *>(prefix.c_str()),
                prefix.length());
            int fd = 0;

            {
                std::lock_guard<std::mutex> lock(g_fd_tree_mutex);
                if (const auto it = g_fd_tree.find(path_crc64); it == g_fd_tree.end()) {
                    if (fd = open(prefix.c_str(), O_RDWR); fd <= 0) {
                        return (errno == 0 ? -EIO : -errno);
                    }
                }
                else {
                    fd = it->second.fd;
                }
            }

            const auto n = std::min(msg->content_length, static_cast<uint64_t>(sizeof(msg->content_data)));
            return write(fd, msg->content_data, n) != n;
        }
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

    g_lock_fs_prefix = fs::current_path().string() + "/" + std::string(argv[1]);
    std::unique_ptr<char*[]> fuse_argv;
    std::vector<std::string> fuse_args;
    if constexpr (DEBUG) {
        // fuse_args.emplace_back("-s");
        // fuse_args.emplace_back("-d");
        fuse_args.emplace_back("-f");
    }

    fuse_args.push_back(argv[2]);
    fuse_args.emplace_back("-o");
    fuse_args.emplace_back("subtype=cfs");
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
