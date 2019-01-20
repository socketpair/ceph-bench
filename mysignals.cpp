#include <csignal>

#include "mysignals.h"
static volatile std::sig_atomic_t gSignalStatus;

static void signal_handler(int signal) { gSignalStatus = signal; }

void setup_signal_handlers() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
}

void abort_if_signalled() {
  if (gSignalStatus)
    throw AbortException();
}
