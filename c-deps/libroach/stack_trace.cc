// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "stack_trace.h"
#include "libroach.h"

#if defined(OS_LINUX) && defined(__GLIBC__)

#include <cxxabi.h>
#include <dirent.h>
#include <execinfo.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <mutex>

namespace {

const int kStackTraceSignal = SIGRTMIN;

// Maximum depth allowed for a stack trace.
const int kMaxDepth = 100;

// Stack trace of a thread.
struct ThreadStack {
  ThreadStack(pid_t id, int fd)
      : tid(id),
        ack_fd(fd) {
  }

  bool Ack() {
    done = true;
    const char ack_ch = 'y';  // the specific value doesn't matter
    int num_written;
    do {
      num_written = write(ack_fd, &ack_ch, sizeof(ack_ch));
    } while (num_written < 0 && errno == EINTR);
    return sizeof(ack_ch) == num_written;
  }

  // ID of the thread to retrieve stack trace from.
  const pid_t tid;
  // File descriptor where the ack should be written.
  const int ack_fd;
  // The stack trace.
  void* addr[kMaxDepth];
  // The depth of the stack trace.
  int depth = 0;
  // Has the stack been populated.
  std::atomic<bool> done;
};

std::vector<pid_t> ListThreads(std::string *error) {
  std::vector<pid_t> pids;
  DIR* dir;
  do {
    dir = opendir("/proc/self/task");
  } while (dir == nullptr && errno == EINTR);
  if (dir == nullptr) {
    *error = "unable to open /proc/self/task";
    return pids;
  }

  for (;;) {
    // NB: readdir_r is deprecated and readdir is actually thread-safe
    // on modern versions of glibc.
    struct dirent* entry = readdir(dir);
    if (entry == nullptr) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    const std::string child(entry->d_name);
    if (child == "." || child == "..") {
      continue;
    }
    auto pid = strtoll(child.c_str(), nullptr, 10);
    pids.push_back(pid_t(pid));
  }

  for (; closedir(dir) < 0 && errno == EINTR; ) {
  }

  if (pids.empty()) {
    *error = "no threads found in /proc/self/task";
  }
  return pids;
}

uint64_t BlockedSignals(pid_t tid, std::string *error) {
  const std::string path = "/proc/" + std::to_string(tid) + "/status";
  int fd;
  do {
    fd = open(path.c_str(), O_RDONLY);
  } while (fd < 0 && errno == EINTR);
  if (fd < -1) {
    *error = path + ": unable to open";
    return 0;
  }
  std::string data;
  for (;;) {
    char buf[1024];
    int n;
    do {
      n = read(fd, buf, sizeof(buf));
    } while (n < 0 && errno == EINTR);
    if (n < 0) {
      *error = path + ": read failed";
      break;
    }
    if (n == 0) {
      break;
    }
    data.append(buf, n);
  }

  for (; close(fd) < 0 && errno == EINTR;) {
  }
  if (!error->empty()) {
    return 0;
  }

  const std::string needle("SigBlk:");
  size_t pos = data.find(needle);
  if (pos == data.npos) {
    *error = path + ": unable to find SigBlk";
    return 0;
  }
  data = data.substr(pos + needle.size());
  return strtoull(data.c_str(), nullptr, 16);
}

void InternalHandler(int signum, siginfo_t* siginfo, void* ucontext) {
  // Ignore signals that were sent by an external process. The
  // stacktrace signal handler is intended only for signals we send to
  // ourselves.
  if (siginfo->si_pid != getpid()) {
    return;
  }
  auto stack = reinterpret_cast<ThreadStack*>(siginfo->si_value.sival_ptr);
  if (stack == nullptr) {
    return;
  }
  stack->depth = backtrace(stack->addr, kMaxDepth);
  stack->Ack();
}

int SignalThread(pid_t pid, pid_t tid, uid_t uid, int signum, sigval payload) {
  // Similar to pthread_sigqueue(), but usable with a tid since we
  // don't have a pthread_t.
  siginfo_t info;
  memset(&info, 0, sizeof(info));
  info.si_signo = signum;
  info.si_code = SI_QUEUE;
  info.si_pid = pid;
  info.si_uid = uid;
  info.si_value = payload;
  return syscall(SYS_rt_tgsigqueueinfo, pid, tid, signum, &info);
}

int64_t NowMillis() {
  timeval tv;
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
}

std::string DumpThreadStacksHelper() {
  std::string error;
  auto tids = ListThreads(&error);
  if (tids.empty()) {
    return error;
  }

  // Create a pipe on which threads can send acks after they finish
  // writing their stacktrace. Since Linux 2.6.11, the default pipe
  // capacity has been 65536. Each thread will be writing a single
  // byte to the pipe, so they should never block.
  int pipe_fd[2];
  if (pipe(pipe_fd) == -1) {
    return "unable to create pipe";
  }

  // Signal all threads to write their stack trace in a pre-allocated
  // area. Note that some threads might have died by now, so
  // signalling them will fail.
  std::vector<std::unique_ptr<ThreadStack>> stacks;
  const auto pid = getpid();
  const auto uid = getuid();
  std::string result;
  char buf[128];
  for (auto tid : tids) {
    std::string error;
    const uint64_t blocked = BlockedSignals(tid, &error);
    if ((blocked & (1ULL << kStackTraceSignal)) != 0) {
      // The thread is blocking receipt of our signal, so don't bother
      // sending it.
      continue;
    }
    if (!error.empty()) {
      snprintf(buf, sizeof(buf), "thread %d\n%s\n\n", tid, error.c_str());
      result.append(buf);
    }

    std::unique_ptr<ThreadStack> stack(new ThreadStack(tid, pipe_fd[1]));
    union sigval payload;
    payload.sival_ptr = stack.get();
    if (SignalThread(pid, tid, uid, kStackTraceSignal, payload) == 0) {
      stacks.push_back(std::move(stack));
    } else {
      snprintf(buf, sizeof(buf), "thread %d\n(no response)\n\n", tid);
      result.append(buf);
    }
  }

  // Set operations on pipe_fd[0] to be non-blocking. This is
  // important if the poll() on this fd returns, but the subsequent
  // read block.
  int flags = fcntl(pipe_fd[0], F_GETFL, 0);
  fcntl(pipe_fd[0], F_SETFL, flags | O_NONBLOCK);

  // Wait for all the acks, timing out after 5 seconds.
  auto end = NowMillis() + 5000;
  for (int acks = 0; acks < stacks.size(); ) {
    pollfd pollfds[1];
    pollfds[0].fd = pipe_fd[0];
    pollfds[0].events = POLLIN;
    pollfds[0].revents = 0;
    auto timeout = end - NowMillis();
    if (timeout <= 0) {
      break;
    }
    auto ret = poll(pollfds, 1, int(timeout));
    if (ret == -1) {
      continue;
    }
    if (ret == 0) {
      // We timed out before reading all of the stacks.
      break;
    }
    if (pollfds[0].revents & POLLIN) {
      char buf[128];
      auto num_read = read(pipe_fd[0], buf, sizeof(buf));
      if (num_read >= 0) {
        acks += num_read;
      }
    }
  }

  close(pipe_fd[0]);
  close(pipe_fd[1]);

  for (auto& stack : stacks) {
    if (!stack->done) {
      // We were unable to populate the stack. This could occur if the
      // signal to the thread was blocked or delayed. In the case of a
      // delayed signal, it could be delivered later, so we need keep
      // the stack around to be populated at that point.
      snprintf(buf, sizeof(buf), "thread %d\n(no response)\n\n", stack->tid);
      result.append(buf);
      stack.release();
      continue;
    }

    snprintf(buf, sizeof(buf), "thread %d\n", stack->tid);
    result.append(buf);

    auto syms = backtrace_symbols(stack->addr, stack->depth);
    for (int i = 2; i < stack->depth; ++i) {
      if (syms != nullptr) {
        // Note that backtrace_symbols includes the address in the
        // output it returns.
        snprintf(buf, sizeof(buf), "#%-2d  %s\n", i-2, syms[i]);
      } else {
        snprintf(buf, sizeof(buf), "#%-2d  0x%08lx\n", i-2, (uintptr_t)(stack->addr[i]));
      }
      result.append(buf);
    }
    result.append("\n");

    if (syms != nullptr) {
      free(syms);
    }
  }

  return result;
}

}  // namespace

std::string DumpThreadStacks() {
  // This code is not thread-safe: ensure we have only one concurrent call to
  // DumpThreadStacks ongoing at a time.
  static std::mutex s_mutex;
  std::lock_guard<std::mutex> lock(s_mutex);

  struct sigaction action;
  struct sigaction oldaction;
  memset(&action, 0, sizeof(action));
  action.sa_sigaction = InternalHandler;
  // Set SA_RESTART so that supported syscalls are automatically restarted if
  // interrupted by the stacktrace collection signal.
  action.sa_flags = SA_ONSTACK | SA_RESTART | SA_SIGINFO;
  if (sigaction(kStackTraceSignal, &action, &oldaction) != 0) {
    return "unable to initialize signal handler";
  }

  auto result = DumpThreadStacksHelper();

  // Restore the old signal handler. We ignore error here as there
  // isn't anything to do if we encounter an error.
  sigaction(kStackTraceSignal, &oldaction, nullptr);

  return result;
}

#else  // !defined(OS_LINUX) || !defined(__GLIBC__)

#include <stdlib.h>

std::string DumpThreadStacks() {
  return "thread stacks only available on Linux/Glibc";
}

#endif // !defined(OS_LINUX) || !defined(__GLIBC__)

// ToDBString converts a std::string to a DBString.
inline DBString ToDBString(const std::string& s) {
  DBString result;
  result.len = s.size();
  result.data = static_cast<char*>(malloc(result.len));
  memcpy(result.data, s.data(), s.size());
  return result;
}

DBString DBDumpThreadStacks() {
  return ToDBString(DumpThreadStacks());
}
